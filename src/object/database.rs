use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::path::Path;

use async_walkdir::WalkDir;
use futures::stream::StreamExt;
use pg_query::protobuf::{node::Node, ConstrType, RangeVar};
use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::postgres::PgDatabaseError;
use sqlx::types::Uuid;
use sqlx::{query_as, query_scalar, Error, PgPool};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::object::{
    find_index, get_constraints, get_extensions, get_functions, get_indexes, get_policies,
    get_schemas, get_sequences, get_tables, get_triggers, get_udts, get_views, is_verbose,
    plpgsql::parse_plpgsql_function, Constraint, Extension, Function, Index, Policy, Schema,
    SchemaQualifiedName, Sequence, SqlObject, SqlObjectEnum, Table, Trigger, Udt, View,
    BUILT_IN_FUNCTIONS, BUILT_IN_NAMES,
};
use crate::PgDiffError;

/// Main object of the application that contains metadata about the targeted database and the source
/// control SQL files provided.
pub struct DatabaseMigration {
    pool: PgPool,
    database: Database,
    source_control_database: SourceControlDatabase,
}

impl DatabaseMigration {
    /// Create a new [DatabaseMigration] using the connection `pool` provided to scrape metadata
    /// from the target database and the `source_control_directory` to collect source control SQL
    /// files for generating the desired new state of the target database.
    ///
    /// # Errors
    /// if database scraping fails (see [Database::from_connection]) or source control file
    /// analyzing fails (see [SourceControlDatabase::from_directory]).
    pub async fn new<P>(pool: PgPool, source_control_directory: P) -> Result<Self, PgDiffError>
    where
        P: AsRef<Path>,
    {
        let database = Database::from_connection(&pool).await?;
        let source_control_database =
            SourceControlDatabase::from_directory(source_control_directory).await?;
        Ok(Self {
            pool,
            database,
            source_control_database,
        })
    }

    /// Plan the steps required to migrate the target database to the state described in the source
    /// control files.
    ///
    /// This applies the source control statements to a temp database, scrapes that temp database
    /// for metadata and compares the temp database to the current state of the target database to
    /// find the steps required for migration.
    ///
    /// # Errors
    /// See [SourceControlDatabase::apply_to_temp_database]
    /// See [SourceControlDatabase::scrape_temp_database]
    pub async fn plan_migration(&mut self) -> Result<String, PgDiffError> {
        self.create_temp_database().await?;
        let db_options = (*self.pool.connect_options())
            .clone()
            .database(&self.source_control_database.temp_db_name);
        let temp_db_pool = PgPool::connect_with(db_options).await?;
        self.source_control_database
            .apply_to_temp_database(&temp_db_pool)
            .await?;
        let source_control_temp_database = Database::from_connection(&temp_db_pool).await?;
        let migration_script = self
            .database
            .compare_to_other_database(&source_control_temp_database)?;
        Ok(migration_script)
    }

    async fn create_temp_database(&self) -> Result<(), PgDiffError> {
        let query = include_str!("./../../queries/check_create_db_role.pgsql");
        let can_create_database: bool = query_scalar(query).fetch_one(&self.pool).await?;
        if !can_create_database {
            return Err("Current user does not have permission to create a temp database for migration staging".into());
        }

        let db_options = DatabaseOptions::from_connection(&self.pool).await?;
        let create_database = format!(
            "CREATE DATABASE {}{};",
            self.source_control_database.temp_db_name, db_options
        );
        sqlx::query(&create_database).execute(&self.pool).await?;
        if is_verbose() {
            println!(
                "Created temp database: {}",
                self.source_control_database.temp_db_name
            );
        }
        Ok(())
    }
}

impl Drop for DatabaseMigration {
    fn drop(&mut self) {
        let db_name = self.source_control_database.temp_db_name.clone();
        let pool = self.pool.clone();
        let fut = async move {
            if let Err(error) = sqlx::query(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE);",
                db_name
            ))
            .execute(&pool)
            .await
            {
                println!("Error dropping temp database: {error}");
            }
        };
        // It's okay to block on this future here since the database migration will signify the end
        // of the application's lifetime
        futures::executor::block_on(fut);
    }
}

struct NodeIter<'n> {
    root: &'n pg_query::NodeEnum,
    current_node: &'n pg_query::NodeEnum,
    queued_elements: VecDeque<SchemaQualifiedName>,
    queued_nodes: VecDeque<&'n pg_query::NodeEnum>,
}

impl<'n> NodeIter<'n> {
    fn new(node: &'n pg_query::NodeEnum) -> Self {
        let mut iter = Self {
            root: node,
            current_node: node,
            queued_elements: VecDeque::new(),
            queued_nodes: VecDeque::new(),
        };
        iter.extract_objects_from_current_node();
        iter
    }

    fn queue_nodes(&mut self, nodes: &'n [pg_query::protobuf::Node]) {
        nodes
            .iter()
            .filter_map(|c| c.node.as_ref())
            .for_each(|n| self.queued_nodes.push_back(n));
    }

    fn queue_node(&mut self, node: &'n Option<Box<pg_query::protobuf::Node>>) {
        if let Some(node) = node.as_deref().and_then(|e| e.node.as_ref()) {
            self.queued_nodes.push_back(node)
        }
    }

    fn queue_names(&mut self, name_nodes: &[pg_query::protobuf::Node]) {
        if let Some(name) = extract_names(name_nodes) {
            self.queued_elements.push_back(name);
        }
    }

    fn queue_relation(&mut self, relation: &Option<RangeVar>) {
        let Some(range_var) = relation else {
            return;
        };
        let name = SchemaQualifiedName::new(&range_var.schemaname, &range_var.relname);
        self.queued_elements.push_back(name);
    }

    fn parse_inline_sql_code(&mut self, code: &str) {
        match pg_query::parse(code) {
            Ok(result) => {
                for table in result.tables() {
                    self.queued_elements.push_back(table.into());
                }
                for function in result.functions() {
                    self.queued_elements.push_back(function.into());
                }
            }
            Err(error) => {
                if is_verbose() {
                    println!(
                        "Skipping SQL code block since the source text could not be parsed. {error}\n{}",
                        code
                    )
                }
            }
        }
    }

    fn parse_inline_plpgsql_code(&mut self, code: &str) {
        match parse_plpgsql_function(code) {
            Ok(functions) => {
                for function in functions {
                    match function.get_objects() {
                        Ok(objects) => {
                            self.queued_elements.append(&mut VecDeque::from(objects));
                        }
                        Err(error) => {
                            if is_verbose() {
                                println!("Skipping plpg/sq; code block since the source text could not be parsed for objects. {error}")
                            }
                        }
                    }
                }
            }
            Err(error) => {
                if is_verbose() {
                    println!("Skipping plpg/sql code block since the source text could not be parsed. {error}\n")
                }
            }
        }
    }

    fn extract_objects_from_current_node(&mut self) -> bool {
        match &self.current_node {
            Node::TableFunc(table_func) => {
                self.queue_nodes(&table_func.coltypes);
                self.queue_node(&table_func.rowexpr)
            }
            Node::CaseExpr(expr) => {
                self.queue_nodes(&expr.args);
                self.queue_node(&expr.defresult);
            }
            Node::CaseWhen(when) => {
                self.queue_node(&when.expr);
                self.queue_node(&when.result);
            }
            Node::TypeName(type_name) => {
                self.queue_nodes(&type_name.names);
            }
            Node::AExpr(expr) => {
                self.queue_node(&expr.lexpr);
                self.queue_node(&expr.lexpr);
            }
            Node::FuncCall(func_call) => {
                self.queue_nodes(&func_call.args);
                self.queue_names(&func_call.funcname);
            }
            Node::ColumnDef(column) => {
                if let Some(name) = &column.type_name {
                    self.queue_names(&name.names);
                }
            }
            Node::AlterTableStmt(alter_table) => {
                self.queue_relation(&alter_table.relation);
                self.queue_nodes(&alter_table.cmds);
            }
            Node::AlterTableCmd(alter_command) => {
                self.queue_node(&alter_command.def);
            }
            Node::CreateStmt(create_table) => {
                self.queue_nodes(&create_table.constraints);
                self.queue_nodes(&create_table.table_elts);
            }
            Node::Constraint(constraint) => match constraint.contype() {
                ConstrType::ConstrCheck => self.queue_node(&constraint.raw_expr),
                ConstrType::ConstrForeign => self.queue_relation(&constraint.pktable),
                _ => {}
            },
            Node::CreatePolicyStmt(create_policy) => {
                self.queue_relation(&create_policy.table);
                self.queue_node(&create_policy.qual);
                self.queue_node(&create_policy.with_check);
            }
            Node::AlterPolicyStmt(alter_policy) => {
                self.queue_relation(&alter_policy.table);
                self.queue_node(&alter_policy.qual);
                self.queue_node(&alter_policy.with_check);
            }
            Node::CreateTrigStmt(create_trigger) => {
                self.queue_relation(&create_trigger.relation);
                self.queue_names(&create_trigger.funcname);
            }
            Node::IndexStmt(index_statement) => {
                self.queue_relation(&index_statement.relation);
            }
            Node::CreateFunctionStmt(create_function) => {
                self.queue_node(&create_function.sql_body);
                if let Some(type_name) = &create_function.return_type {
                    self.queue_names(&type_name.names);
                }
                self.queue_nodes(&create_function.parameters);
                let def_elements = create_function
                    .options
                    .iter()
                    .filter_map(|n| n.node.as_ref())
                    .filter_map(|n| {
                        if let Node::DefElem(def_element) = n {
                            Some(def_element)
                        } else {
                            None
                        }
                    });
                if let Some(language) = def_elements
                    .clone()
                    .filter(|def| def.defname == "language")
                    .filter_map(|def| def.arg.as_ref().and_then(|a| a.node.as_ref()))
                    .filter_map(|n| {
                        if let Node::String(language) = n {
                            Some(language)
                        } else {
                            None
                        }
                    })
                    .next()
                {
                    match language.sval.as_str() {
                        "plpgsql" => match self.current_node.deparse() {
                            Ok(function_def) => self.parse_inline_plpgsql_code(&function_def),
                            Err(error) => {
                                if is_verbose() {
                                    println!("Could not deparse plpg/sql function. {error}");
                                }
                            }
                        },
                        "sql" => {
                            if let Some(inline_code) = def_elements
                                .clone()
                                .filter(|def| def.defname == "as")
                                .filter_map(|def| def.arg.as_ref().and_then(|a| a.node.as_ref()))
                                .filter_map(|n| {
                                    if let Node::List(list) = n {
                                        Some(list.items.iter())
                                    } else {
                                        None
                                    }
                                })
                                .flatten()
                                .filter_map(|n| n.node.as_ref())
                                .filter_map(|n| {
                                    if let Node::String(inline_code) = n {
                                        Some(inline_code)
                                    } else {
                                        None
                                    }
                                })
                                .next()
                            {
                                self.parse_inline_sql_code(&inline_code.sval)
                            }
                        }
                        _ => {
                            if is_verbose() {
                                println!(
                                    "Unknown language '{}' for function. Could not parse.",
                                    language.sval
                                )
                            }
                        }
                    }
                };
            }
            Node::FunctionParameter(function_parameter) => {
                if let Some(type_name) = &function_parameter.arg_type {
                    self.queue_names(&type_name.names);
                }
            }
            Node::AlterFunctionStmt(alter_function) => {
                self.queue_nodes(&alter_function.actions);
            }
            Node::InlineCodeBlock(inline_code_block) => match inline_code_block.lang_oid {
                14 => self.parse_inline_sql_code(&inline_code_block.source_text),
                13545 => self.parse_inline_plpgsql_code(&inline_code_block.source_text),
                _ => {
                    if is_verbose() {
                        println!(
                            "Skipping code block since the language is not supported. Lang ID = {}",
                            inline_code_block.lang_oid
                        )
                    }
                }
            },
            Node::AlterTypeStmt(alter_type) => {
                self.queue_nodes(&alter_type.options);
            }
            Node::CompositeTypeStmt(composite_type) => {
                self.queue_nodes(&composite_type.coldeflist);
            }
            Node::ViewStmt(view) => {
                if let Some(query) = view.query.as_ref().and_then(|q| q.node.as_ref()) {
                    match query.deparse() {
                        Ok(query_text) => self.parse_inline_sql_code(&query_text),
                        Err(error) => {
                            if is_verbose() {
                                println!("Error trying to deparse view query. {error}")
                            }
                        }
                    }
                }
            }
            _ => return false,
        };
        self.move_to_next_node();
        true
    }

    fn move_to_next_node(&mut self) {
        if let Some(node) = self.queued_nodes.pop_front() {
            self.current_node = node;
        } else {
            self.current_node = self.root;
        }
    }

    fn get_next(&mut self) -> Option<SchemaQualifiedName> {
        if let Some(name) = self.queued_elements.pop_front() {
            return Some(name);
        }
        if self.current_node == self.root {
            return None;
        }
        if self.extract_objects_from_current_node() {
            return self.get_next();
        }
        self.move_to_next_node();
        self.get_next()
    }
}

impl<'n> Iterator for NodeIter<'n> {
    type Item = SchemaQualifiedName;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next()
    }
}

#[derive(Debug, PartialEq, Clone)]
struct DdlStatement {
    statement: String,
    object: SchemaQualifiedName,
    dependencies: Vec<SchemaQualifiedName>,
}

impl DdlStatement {
    fn has_dependencies_met(&self, completed_dependencies: &HashSet<SchemaQualifiedName>) -> bool {
        self.dependencies
            .iter()
            .all(|d| completed_dependencies.contains(d))
    }

    fn depends_on(&self, object: &SchemaQualifiedName) -> bool {
        self.dependencies.contains(object)
    }
}

struct StatementIter {
    statements: Vec<DdlStatement>,
    completed_objects: HashSet<SchemaQualifiedName>,
    failed_statements: Vec<DdlStatement>,
    initial_failed_count: usize,
    failed_statement_index: usize,
}

impl StatementIter {
    fn new(statements: &[DdlStatement]) -> Self {
        Self {
            statements: statements.to_vec(),
            completed_objects: HashSet::new(),
            failed_statements: vec![],
            initial_failed_count: 0,
            failed_statement_index: 0,
        }
    }

    fn add_back_failed_statement(&mut self, statement: DdlStatement) {
        self.completed_objects.remove(&statement.object);
        self.failed_statements.push(statement);
    }

    fn has_remaining(&self) -> bool {
        !self.statements.is_empty() || !self.failed_statements.is_empty()
    }

    fn take_remaining(&mut self) -> Vec<DdlStatement> {
        let mut result = vec![];
        result.append(&mut self.statements);
        result.append(&mut self.failed_statements);
        result
    }
}

impl Iterator for StatementIter {
    type Item = DdlStatement;

    fn next(&mut self) -> Option<Self::Item> {
        if self.statements.is_empty() && self.failed_statements.is_empty() {
            return None;
        }

        if !self.statements.is_empty() {
            if let Some(index) = find_index(&self.statements, |s| {
                s.has_dependencies_met(&self.completed_objects)
            }) {
                let statement = self.statements.remove(index);
                self.completed_objects.insert(statement.object.clone());
                return Some(statement);
            }
            if let Some(index) = find_index(&self.statements, |s| {
                self.statements
                    .iter()
                    .all(|other| !s.depends_on(&other.object))
            }) {
                let statement = self.statements.remove(index);
                self.completed_objects.insert(statement.object.clone());
                return Some(statement);
            }
            return Some(self.statements.remove(0));
        }

        if self.initial_failed_count == 0 {
            self.initial_failed_count = self.failed_statements.len();
            return Some(self.failed_statements.remove(self.failed_statement_index));
        }

        self.failed_statement_index += 1;
        self.failed_statement_index = self
            .failed_statement_index
            .clamp(0, self.failed_statements.len() - 1);
        if self.failed_statement_index == 0 {
            if self.initial_failed_count == self.failed_statements.len() {
                return None;
            }
            self.initial_failed_count = self.failed_statements.len();
        }
        Some(self.failed_statements.remove(self.failed_statement_index))
    }
}

#[derive(Debug)]
pub struct SourceControlDatabase {
    temp_db_name: String,
    statements: Vec<DdlStatement>,
}

impl SourceControlDatabase {
    fn new() -> Self {
        Self {
            temp_db_name: format!(
                "pg_diff_rs_{}",
                Uuid::new_v4().to_string().replace("-", "_")
            ),
            statements: vec![],
        }
    }

    pub async fn from_directory<P>(files_path: P) -> Result<Self, PgDiffError>
    where
        P: AsRef<Path>,
    {
        println!("Analyzing code within source control directory");
        let mut builder = SourceControlDatabase::new();
        let mut entries = WalkDir::new(files_path).map(|entry| entry.map(|e| e.path()));
        while let Some(result) = entries.next().await {
            let path = result?;
            if path.is_dir() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|f| f.to_str()) else {
                if is_verbose() {
                    println!("Skipping {:?}", path);
                }
                continue;
            };
            if !file_name.ends_with(".pgsql") && !file_name.ends_with(".sql") {
                if is_verbose() {
                    println!("Skipping {:?}", file_name);
                }
                continue;
            }
            builder.append_source_file(path).await?;
        }
        println!("Done!");
        Ok(builder)
    }

    /// Read source file and find all queries, the main DDL object of each query and the
    /// dependencies for each DDL query.
    ///
    /// The steps are as follows:
    /// 1. Read the entire source control file into a string buffer.
    /// 2. Split the source file statements into 1 or more queries.
    /// 3. Parse each query extracting:
    ///     * Root node of the query for further analyzing
    ///     * Main object created/altered by the query (found from the root node)
    ///     * All dependencies of the query (found by expanding [NodeIter])
    ///
    /// # Errors
    /// If an IO error occurs trying to read the file path or an error occurs attempting to read the
    /// AST returned from query parsing. Querying parsing can fail for various reasons, but it
    /// should only fail if the SQL code is not syntactically valid.
    async fn append_source_file<P>(&mut self, path: P) -> Result<(), PgDiffError>
    where
        P: AsRef<Path>,
    {
        let mut file = File::open(path.as_ref()).await?;
        let mut str = String::new();
        file.read_to_string(&mut str).await?;
        let Some(file_name) = path.as_ref().file_stem().and_then(|f| f.to_str()) else {
            return Err(PgDiffError::General(format!(
                "Could not extract a file name from {:?}",
                path.as_ref()
            )));
        };
        let queries = pg_query::split_with_parser(&str).map_err(|error| PgDiffError::PgQuery {
            object_name: file_name.into(),
            error,
        })?;
        for query in queries {
            let result = pg_query::parse(query).map_err(|error| PgDiffError::PgQuery {
                object_name: file_name.into(),
                error,
            })?;
            let root_node = result
                .protobuf
                .stmts
                .first()
                .and_then(|s| s.stmt.as_ref())
                .and_then(|n| n.node.as_ref());
            let root_node = *extract_option(
                &path,
                &root_node,
                format!(
                    "Could not get first node of statement: {:?}",
                    result.protobuf
                ),
            )?;
            let parent_object = match root_node {
                Node::AlterTableStmt(alter_table) => {
                    let relation = extract_option(
                        &path,
                        &alter_table.relation,
                        "Could not extract a table name from from an ALTER TABLE statement".into(),
                    )?;
                    let constraint_names = alter_table
                        .cmds
                        .iter()
                        .filter_map(|n| n.node.as_ref())
                        .filter_map(|n| match n {
                            Node::Constraint(constraint) => Some(constraint.conname.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<&str>>()
                        .join(",");

                    SchemaQualifiedName::new(
                        &relation.schemaname,
                        &format!("{}.({})", relation.relname, constraint_names),
                    )
                }
                Node::CreateSchemaStmt(create_schema) => {
                    SchemaQualifiedName::new(&create_schema.schemaname, "")
                }
                Node::CompositeTypeStmt(composite_type) => {
                    let composite = extract_option(
                        &path,
                        &composite_type.typevar,
                        "Could not extract a table name from from an CREATE POLICY statement"
                            .into(),
                    )?;
                    SchemaQualifiedName::new(&composite.schemaname, &composite.relname)
                }
                Node::CreateExtensionStmt(create_extension) => {
                    SchemaQualifiedName::new("", &create_extension.extname)
                }
                Node::CreatePolicyStmt(create_policy) => {
                    let relation = extract_option(
                        &path,
                        &create_policy.table,
                        "Could not extract a table name from from an CREATE POLICY statement"
                            .into(),
                    )?;
                    SchemaQualifiedName::new(
                        &relation.schemaname,
                        &format!("{}.{}", relation.relname, create_policy.policy_name),
                    )
                }
                Node::CreateTrigStmt(create_trigger) => {
                    let relation = extract_option(
                        &path,
                        &create_trigger.relation,
                        "Could not extract a table name from from an CREATE TRIGGER statement"
                            .into(),
                    )?;
                    SchemaQualifiedName::new(
                        &relation.schemaname,
                        &format!("{}.{}", relation.relname, create_trigger.trigname),
                    )
                }
                Node::CreateSeqStmt(create_sequence) => {
                    let sequence = extract_option(
                        &path,
                        &create_sequence.sequence,
                        "Could not extract a table name from from an CREATE SEQUENCE statement"
                            .into(),
                    )?;
                    SchemaQualifiedName::new(&sequence.schemaname, &sequence.relname)
                }
                Node::CreateFunctionStmt(create_function) => {
                    extract_names(&create_function.funcname).ok_or(PgDiffError::FileQueryParse {
                        path: path.as_ref().into(),
                        message: "Could not extract function name".into(),
                    })?
                }
                Node::CreateEnumStmt(create_enum) => {
                    extract_names(&create_enum.type_name).ok_or(PgDiffError::FileQueryParse {
                        path: path.as_ref().into(),
                        message: "Could not extract enum type name".into(),
                    })?
                }
                Node::CreateRangeStmt(create_range) => extract_names(&create_range.type_name)
                    .ok_or(PgDiffError::FileQueryParse {
                        path: path.as_ref().into(),
                        message: "Could not extract range type name".into(),
                    })?,
                Node::CreateStmt(create_table) => {
                    let relation = extract_option(
                        &path,
                        &create_table.relation,
                        "Could not extract a table name from from an CREATE TABLE statement".into(),
                    )?;
                    SchemaQualifiedName::new(&relation.schemaname, &relation.relname)
                }
                Node::ViewStmt(create_view) => {
                    let relation = extract_option(
                        &path,
                        &create_view.view,
                        "Could not extract a view name from from an CREATE VIEW statement".into(),
                    )?;
                    SchemaQualifiedName::new(&relation.schemaname, &relation.relname)
                }
                Node::IndexStmt(create_index) => {
                    let relation = extract_option(
                        &path,
                        &create_index.relation,
                        "Could not extract a view name from from an CREATE VIEW statement".into(),
                    )?;
                    SchemaQualifiedName::new(&relation.schemaname, &create_index.idxname)
                }
                _ => {
                    return Err(PgDiffError::FileQueryParse {
                        path: path.as_ref().into(),
                        message: format!(
                            "First node of statement is not recognized: {:?}",
                            root_node
                        ),
                    });
                }
            };
            let statement = DdlStatement {
                statement: query.to_string(),
                object: parent_object,
                dependencies: NodeIter::new(root_node).collect(),
            };
            self.statements.push(statement);
        }

        Ok(())
    }

    /// Apply statements collected from SQL source control files and apply them to the database
    /// targeted by the supplied `pool`.
    ///
    /// This creates a [StatementIter] referencing the `statements` collected. The order of
    /// statements follows all objects whose dependencies have already been met (or no dependencies
    /// exist). As objects are created, the collection of dependencies created will be updated and
    /// statements that were previously not able to created, are then released for execution. If
    /// an error occurs during query execution, the statement is put into a special queue of failed
    /// statements that will be handled later. For failed statements, the error message is also
    /// checked to see if the missing dependency is specified and if found, the dependency is added
    /// to the object's list of dependencies before pushing to error statement queue.
    ///
    /// After iteration completes, the iterator object is checked to see if any statements remain
    /// which would indicate some of the statements could not be executed successfully (i.e. a
    /// circular dependency was found or the application could not derive the order of the
    /// statements to create the database state).
    ///
    /// For more details of iteration, see [StatementIter].
    ///
    /// # Errors
    /// - Executing the statement query returns an error that cannot be parsed into a
    ///     [PgDatabaseError]
    /// - After iterating over the ordered statements, the iterator still has remaining statements.
    ///     This would indicate that an infinite loop was detected and the application cannot
    ///     continue
    pub async fn apply_to_temp_database(&mut self, pool: &PgPool) -> Result<(), PgDiffError> {
        println!("Applying source control DDL statements to temp database");
        println!("Temp Database Name: {}", self.temp_db_name);
        println!("Total statements: {}", self.statements.len());

        let mut iter = StatementIter::new(&self.statements);
        let mut i = 0;
        while let Some(statement) = iter.next() {
            if let Err(error) = sqlx::query(&statement.statement).execute(pool).await {
                let Error::Database(db_error) = &error else {
                    return Err(error.into());
                };
                let Some(pg_error) = db_error.try_downcast_ref::<PgDatabaseError>() else {
                    return Err(error.into());
                };
                let Some(item) = self.statements.iter_mut().find(|s| **s == statement) else {
                    iter.add_back_failed_statement(statement);
                    continue;
                };
                let message = pg_error.message();
                if message.ends_with(" does not exist") {
                    let name: String = message
                        .chars()
                        .skip_while(|c| *c == '"')
                        .take_while(|c| *c == '"')
                        .collect();
                    let dependency = SchemaQualifiedName::from(name.trim_matches('"'));
                    item.dependencies.push(dependency);
                }
                iter.add_back_failed_statement(item.clone());
                continue;
            }
            i += 1;
            if is_verbose() {
                println!("Statement {}/{}\n", i, self.statements.len());
            }
        }
        if iter.has_remaining() {
            let remaining_statements = iter
                .take_remaining()
                .into_iter()
                .map(|s| s.statement)
                .collect();
            return Err(PgDiffError::SourceControlScript {
                remaining_statements,
            });
        }
        println!("Done!");
        Ok(())
    }
}

/// Extract the schema qualified name(s) from the list of `name_nodes` supplied. This assumes that
/// each list item node is a node containing a [Node::String].
///
/// Returns a [SchemaQualifiedName] if a name can be extracted. Returns [None] when:
/// - the schema name is `pg_catalog`
/// - the name has no schema + the local name is in [BUILT_IN_NAMES] or [BUILT_IN_FUNCTIONS]
/// - there are no nodes in the list
///
/// See [extract_string].
fn extract_names(name_nodes: &[pg_query::protobuf::Node]) -> Option<SchemaQualifiedName> {
    match name_nodes {
        [schema_name, local_name] => {
            let schema_name = extract_string(schema_name)?;
            if schema_name == "pg_catalog" {
                return None;
            }
            let local_name = extract_string(local_name)?;
            Some(SchemaQualifiedName::new(schema_name, local_name))
        }
        [local_name] => {
            let local_name = extract_string(local_name)?;
            if BUILT_IN_NAMES.contains(&local_name.as_str())
                || BUILT_IN_FUNCTIONS.contains(&local_name.as_str())
            {
                return None;
            }
            Some(SchemaQualifiedName::from(local_name))
        }
        _ => None,
    }
}

/// Extract the string contained within the `node`. Returns [None] if the `node` does not point to
/// anything or the inner node is not [Node::String]. Otherwise, the inner string is returned.
fn extract_string(node: &pg_query::protobuf::Node) -> Option<&String> {
    match &node.node {
        Some(pg_query::NodeEnum::String(pg_query::protobuf::String { sval, .. })) => Some(sval),
        _ => None,
    }
}

/// Extract a reference to the value within the `option` if it's [Some]. If the value is [None],
/// return a [PgDiffError::FileQueryParse] with the `path` and `message`.
fn extract_option<P, I>(path: P, option: &Option<I>, message: String) -> Result<&I, PgDiffError>
where
    P: AsRef<Path>,
{
    option.as_ref().ok_or(PgDiffError::FileQueryParse {
        path: path.as_ref().into(),
        message,
    })
}

/// Contains the LOCAL_PROVIDER database option as well as the other allowed options based upon the
/// specific provider type.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum LocalProvider {
    #[serde(rename = "c")]
    Libc {
        lc_collate: String,
        lc_ctype: String,
    },
    #[serde(rename = "i")]
    Icu {
        icu_locale: String,
        icu_rules: Option<String>,
    },
}

impl Display for LocalProvider {
    /// Write the LOCAL_PROVIDER option as well as the other options allowed with the specific
    /// [LocalProvider]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalProvider::Libc {
                lc_collate,
                lc_ctype,
            } => {
                write!(
                    f,
                    "\n    LOCALE_PROVIDER 'libc'\n    LC_COLLATE '{}'\n    LC_CTYPE '{}'",
                    lc_collate, lc_ctype
                )
            }
            LocalProvider::Icu {
                icu_locale,
                icu_rules,
            } => {
                write!(
                    f,
                    "\n    LOCALE_PROVIDER 'icu'\n    ICU_LOCALE '{}'",
                    icu_locale
                )?;
                if let Some(icu_rules) = icu_rules {
                    write!(f, "\n    ICU_RULES '{}'", icu_rules)?;
                }
                Ok(())
            }
        }
    }
}

/// Database options that can be supplied when executing a `CREATE DATABASE` command
#[derive(Debug, sqlx::FromRow)]
struct DatabaseOptions {
    encoding: String,
    locale: Option<String>,
    #[sqlx(json)]
    locale_provider: LocalProvider,
    collation_version: String,
}

impl Display for DatabaseOptions {
    /// Write the `CREATE DATABASE` options specified by this struct
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " WITH\n    ENCODING '{}'\n    COLLATION_VERSION '{}'",
            self.encoding, self.collation_version
        )?;
        if let Some(locale) = &self.locale {
            write!(f, "\n    LOCALE '{}'", locale)?;
        }
        write!(f, "{}", self.locale_provider)
    }
}

impl DatabaseOptions {
    /// Capture the pool's current database's options
    async fn from_connection(pool: &PgPool) -> Result<Self, PgDiffError> {
        let query = include_str!("./../../queries/database.pgsql");
        let db_options = query_as(query).fetch_one(pool).await?;
        Ok(db_options)
    }
}

/// Struct representing all database objects that can be found within a target database. This
/// ignores objects that are directly owned by extensions and does not include the public schema
/// which is already present within a database.
#[derive(Debug)]
pub struct Database {
    pub(crate) schemas: Vec<Schema>,
    pub(crate) udts: Vec<Udt>,
    pub(crate) tables: Vec<Table>,
    pub(crate) policies: Vec<Policy>,
    pub(crate) constraints: Vec<Constraint>,
    pub(crate) indexes: Vec<Index>,
    pub(crate) triggers: Vec<Trigger>,
    pub(crate) sequences: Vec<Sequence>,
    pub(crate) functions: Vec<Function>,
    pub(crate) views: Vec<View>,
    pub(crate) extensions: Vec<Extension>,
}

impl Database {
    /// Create a new [Database] from the database targeted by the supplied `pool`.
    ///
    /// Collect all available metadata about the database form the `pg_catalog` tables/views as well
    /// as attempting to analyze non-compiled functions (i.e. dynamic sql and pl/pgsql functions)
    /// to figured out dependencies. Function analysis is not guaranteed to work so errors are
    /// written to STDOUT if the verbose flag is active.
    ///
    /// # Errors
    /// - Errors from the SQL queries executed to fetch metadata
    /// - SQL query parsing if a function is a dynamic SQL query but the query is invalid
    /// - A function is not SQL or pl/pgsql (other languages are not supported)
    pub async fn from_connection(pool: &PgPool) -> Result<Self, PgDiffError> {
        println!(
            "Scraping database {} for metadata",
            pool.connect_options().get_database().unwrap_or_default()
        );
        let mut schemas = get_schemas(pool).await?;
        let schema_names: Vec<&str> = schemas
            .iter()
            .map(|s| s.name.schema_name.as_str())
            .collect();
        let udts = get_udts(pool, &schema_names).await?;
        let tables = get_tables(pool, &schema_names).await?;
        let table_oids: Vec<Oid> = tables.iter().map(|t| t.oid).collect();
        let policies = get_policies(pool, &table_oids).await?;
        let constraints = get_constraints(pool, &table_oids).await?;
        let indexes = get_indexes(pool, &table_oids).await?;
        let triggers = get_triggers(pool, &table_oids).await?;
        let sequences = get_sequences(pool, &schema_names).await?;
        let functions = get_functions(pool, &schema_names).await?;
        let views = get_views(pool, &schema_names).await?;
        if let Some(index) = find_index(&schemas, |schema| schema.name.schema_name == "public") {
            schemas.remove(index);
        }
        let mut database = Database {
            schemas,
            udts,
            tables,
            policies,
            constraints,
            indexes,
            triggers,
            sequences,
            functions,
            views,
            extensions: get_extensions(pool).await?,
        };
        for function in database.functions.iter_mut() {
            function.extract_more_dependencies(pool).await?;
        }
        println!("Done!");
        Ok(database)
    }

    /// Use the metadata scraped from the database to create SQL source control files in the
    /// `output_path` provided.
    ///
    /// This creates files in subdirectories:
    /// - schema, 1 per schema
    /// - extension, 1 per extension
    /// - composite, 1 per composite UDT
    /// - enum, 1 per enum UDT
    /// - table, 1 per table with all constraints, indexes, triggers and policies owned by the table
    ///     included in this file
    /// - view, 1 per view
    /// - sequence, 1 per sequence
    /// - function, 1 per function
    /// - procedure, 1 per procedure
    ///
    /// # Errors
    /// - General format errors when attempting to write the statements to a string buffer
    /// - General IO errors when writing the string buffer to the file
    ///
    /// See [write_create_statements_to_file]
    /// See [append_create_statements_to_owner_table_file]
    pub async fn script_out<P>(&self, output_path: P) -> Result<(), PgDiffError>
    where
        P: AsRef<Path>,
    {
        for schema in &self.schemas {
            write_create_statements_to_file(schema, &output_path).await?;
        }
        for extension in &self.extensions {
            write_create_statements_to_file(extension, &output_path).await?;
        }
        for udt in &self.udts {
            write_create_statements_to_file(udt, &output_path).await?;
        }
        for table in &self.tables {
            write_create_statements_to_file(table, &output_path).await?;
            for constraint in self.constraints.iter().filter(|c| c.table_oid == table.oid) {
                append_create_statements_to_owner_table_file(
                    constraint,
                    &constraint.owner_table_name,
                    &output_path,
                )
                .await?
            }
            for index in self.indexes.iter().filter(|i| i.table_oid == table.oid) {
                append_create_statements_to_owner_table_file(
                    index,
                    &index.owner_table_name,
                    &output_path,
                )
                .await?
            }
            for trigger in self.triggers.iter().filter(|t| t.table_oid == table.oid) {
                append_create_statements_to_owner_table_file(
                    trigger,
                    &trigger.owner_table_name,
                    &output_path,
                )
                .await?
            }
            for policy in self.policies.iter().filter(|c| c.table_oid == table.oid) {
                append_create_statements_to_owner_table_file(
                    policy,
                    &policy.owner_table_name,
                    &output_path,
                )
                .await?
            }
        }
        for view in &self.views {
            write_create_statements_to_file(view, &output_path).await?;
        }
        for sequence in &self.sequences {
            if let Some(owner_table) = &sequence.owner {
                append_create_statements_to_owner_table_file(
                    sequence,
                    &owner_table.table_name,
                    &output_path,
                )
                .await?;
            } else {
                write_create_statements_to_file(sequence, &output_path).await?;
            }
        }
        for function in &self.functions {
            write_create_statements_to_file(function, &output_path).await?;
        }
        Ok(())
    }

    /// Compare this database to another database. Assumes the other database is the desired state
    /// of the database and this object is the current state that needs to be migrated.
    fn compare_to_other_database(&self, other: &Self) -> Result<String, PgDiffError> {
        println!("Comparing source control database to actual database");
        let mut result = String::new();
        for obj in DbCompare::new(self, other) {
            match obj {
                DbCompareResult::Create(new) => new.create_statements(&mut result)?,
                DbCompareResult::Alter { old, new } => {
                    old.alter_statements(&new, &mut result)?;
                }
                DbCompareResult::Drop(old) => old.drop_statements(&mut result)?,
            }
        }
        println!("Done!");
        Ok(result)
    }
}

struct DbIter<'d> {
    database: &'d Database,
    completed_objects: Vec<&'d SchemaQualifiedName>,
    completed_schemas: usize,
    completed_extensions: usize,
    completed_udt: usize,
    completed_tables: usize,
    completed_constraints: usize,
    completed_indexes: usize,
    completed_triggers: usize,
    completed_policies: usize,
    completed_views: usize,
    completed_sequences: usize,
    completed_functions: usize,
}

impl<'d> DbIter<'d> {
    fn new(database: &'d Database) -> Self {
        Self {
            database,
            completed_objects: vec![],
            completed_schemas: 0,
            completed_extensions: 0,
            completed_udt: 0,
            completed_tables: 0,
            completed_constraints: 0,
            completed_indexes: 0,
            completed_triggers: 0,
            completed_policies: 0,
            completed_views: 0,
            completed_sequences: 0,
            completed_functions: 0,
        }
    }
}

impl<'d> Iterator for DbIter<'d> {
    type Item = SqlObjectEnum<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.completed_schemas < self.database.schemas.len() {
            if let Some(schema) = self.database.schemas.iter().find(|s| {
                !self.completed_objects.contains(&&s.name)
                    && s.dependencies_met(&self.completed_objects)
            }) {
                self.completed_schemas += 1;
                self.completed_objects.push(&schema.name);
                return Some(SqlObjectEnum::Schema(schema));
            }
        }

        if self.completed_extensions < self.database.extensions.len() {
            if let Some(extension) = self.database.extensions.iter().find(|e| {
                !self.completed_objects.contains(&&e.name)
                    && e.dependencies_met(&self.completed_objects)
            }) {
                self.completed_extensions += 1;
                self.completed_objects.push(&extension.name);
                return Some(SqlObjectEnum::Extension(extension));
            }
        }

        if self.completed_udt < self.database.udts.len() {
            if let Some(udt) = self.database.udts.iter().find(|u| {
                !self.completed_objects.contains(&&u.name)
                    && u.dependencies_met(&self.completed_objects)
            }) {
                self.completed_udt += 1;
                self.completed_objects.push(&udt.name);
                return Some(SqlObjectEnum::Udt(udt));
            }
        }

        if self.completed_tables < self.database.tables.len() {
            if let Some(table) = self.database.tables.iter().find(|t| {
                !self.completed_objects.contains(&&t.name)
                    && t.dependencies_met(&self.completed_objects)
            }) {
                self.completed_tables += 1;
                self.completed_objects.push(&table.name);
                return Some(SqlObjectEnum::Table(table));
            }
        }

        if self.completed_constraints < self.database.constraints.len() {
            if let Some(constraint) = self.database.constraints.iter().find(|c| {
                !self.completed_objects.contains(&&c.schema_qualified_name)
                    && c.dependencies_met(&self.completed_objects)
            }) {
                self.completed_constraints += 1;
                self.completed_objects
                    .push(&constraint.schema_qualified_name);
                return Some(SqlObjectEnum::Constraint(constraint));
            }
        }

        if self.completed_indexes < self.database.indexes.len() {
            if let Some(index) = self.database.indexes.iter().find(|i| {
                !self.completed_objects.contains(&&i.schema_qualified_name)
                    && i.dependencies_met(&self.completed_objects)
            }) {
                self.completed_indexes += 1;
                self.completed_objects.push(&index.schema_qualified_name);
                return Some(SqlObjectEnum::Index(index));
            }
        }

        if self.completed_triggers < self.database.triggers.len() {
            if let Some(trigger) = self.database.triggers.iter().find(|t| {
                !self.completed_objects.contains(&&t.schema_qualified_name)
                    && t.dependencies_met(&self.completed_objects)
            }) {
                self.completed_triggers += 1;
                self.completed_objects.push(&trigger.schema_qualified_name);
                return Some(SqlObjectEnum::Trigger(trigger));
            }
        }

        if self.completed_policies < self.database.policies.len() {
            if let Some(policy) = self.database.policies.iter().find(|s| {
                !self.completed_objects.contains(&&s.schema_qualified_name)
                    && s.dependencies_met(&self.completed_objects)
            }) {
                self.completed_policies += 1;
                self.completed_objects.push(&policy.schema_qualified_name);
                return Some(SqlObjectEnum::Policy(policy));
            }
        }

        if self.completed_views < self.database.views.len() {
            if let Some(view) = self.database.views.iter().find(|v| {
                !self.completed_objects.contains(&&v.name)
                    && v.dependencies_met(&self.completed_objects)
            }) {
                self.completed_views += 1;
                self.completed_objects.push(&view.name);
                return Some(SqlObjectEnum::View(view));
            }
        }

        if self.completed_sequences < self.database.sequences.len() {
            if let Some(sequence) = self.database.sequences.iter().find(|s| {
                !self.completed_objects.contains(&&s.name)
                    && s.dependencies_met(&self.completed_objects)
            }) {
                self.completed_sequences += 1;
                self.completed_objects.push(&sequence.name);
                return Some(SqlObjectEnum::Sequence(sequence));
            }
        }

        if self.completed_functions < self.database.functions.len() {
            if let Some(function) = self.database.functions.iter().find(|f| {
                !self.completed_objects.contains(&&f.name)
                    && f.dependencies_met(&self.completed_objects)
            }) {
                self.completed_functions += 1;
                self.completed_objects.push(&function.name);
                return Some(SqlObjectEnum::Function(function));
            }
        }
        None
    }
}

enum DbCompareResult<'d> {
    Create(SqlObjectEnum<'d>),
    Alter {
        old: SqlObjectEnum<'d>,
        new: SqlObjectEnum<'d>,
    },
    Drop(SqlObjectEnum<'d>),
}

struct DbCompare<'d> {
    old: &'d Database,
    new: &'d Database,
    old_iter: DbIter<'d>,
    new_iter: DbIter<'d>,
    is_done_old: bool,
}

impl<'d> DbCompare<'d> {
    fn new(old: &'d Database, new: &'d Database) -> Self {
        Self {
            old,
            new,
            old_iter: DbIter::new(old),
            new_iter: DbIter::new(new),
            is_done_old: false,
        }
    }
}

impl<'d> Iterator for DbCompare<'d> {
    type Item = DbCompareResult<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done_old {
            let obj = self.new_iter.next()?;
            return Some(DbCompareResult::Create(obj));
        }

        let Some(obj) = self.old_iter.next() else {
            self.is_done_old = true;
            return self.next();
        };

        let new_object = match obj {
            SqlObjectEnum::Schema(schema) => self.new.schemas.iter().find_map(|s| {
                if s.name() == schema.name() {
                    Some(SqlObjectEnum::Schema(s))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Extension(extension) => self.new.extensions.iter().find_map(|e| {
                if e.name() == extension.name() {
                    Some(SqlObjectEnum::Extension(e))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Udt(udt) => self.new.udts.iter().find_map(|u| {
                if u.name() == udt.name() {
                    Some(SqlObjectEnum::Udt(u))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Table(table) => self.new.tables.iter().find_map(|t| {
                if t.name() == table.name() {
                    Some(SqlObjectEnum::Table(t))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Policy(policy) => self.new.policies.iter().find_map(|p| {
                if p.name() == policy.name() {
                    Some(SqlObjectEnum::Policy(p))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Constraint(constraint) => self.new.constraints.iter().find_map(|c| {
                if c.name() == constraint.name() {
                    Some(SqlObjectEnum::Constraint(c))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Index(index) => self.new.indexes.iter().find_map(|i| {
                if i.name() == index.name() {
                    Some(SqlObjectEnum::Index(i))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Trigger(trigger) => self.new.triggers.iter().find_map(|t| {
                if t.name() == trigger.name() {
                    Some(SqlObjectEnum::Trigger(t))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Sequence(sequence) => self.new.sequences.iter().find_map(|s| {
                if s.name() == sequence.name() {
                    Some(SqlObjectEnum::Sequence(s))
                } else {
                    None
                }
            }),
            SqlObjectEnum::Function(function) => self.new.functions.iter().find_map(|f| {
                if f.name() == function.name() {
                    Some(SqlObjectEnum::Function(f))
                } else {
                    None
                }
            }),
            SqlObjectEnum::View(view) => self.new.views.iter().find_map(|v| {
                if v.name() == view.name() {
                    Some(SqlObjectEnum::View(v))
                } else {
                    None
                }
            }),
        };

        if let Some(other) = new_object {
            match &other {
                SqlObjectEnum::Schema(_) => self.new_iter.completed_schemas += 1,
                SqlObjectEnum::Extension(_) => self.new_iter.completed_extensions += 1,
                SqlObjectEnum::Udt(_) => self.new_iter.completed_udt += 1,
                SqlObjectEnum::Table(_) => self.new_iter.completed_tables += 1,
                SqlObjectEnum::Policy(_) => self.new_iter.completed_policies += 1,
                SqlObjectEnum::Constraint(_) => self.new_iter.completed_constraints += 1,
                SqlObjectEnum::Index(_) => self.new_iter.completed_indexes += 1,
                SqlObjectEnum::Trigger(_) => self.new_iter.completed_triggers += 1,
                SqlObjectEnum::Sequence(_) => self.new_iter.completed_sequences += 1,
                SqlObjectEnum::Function(_) => self.new_iter.completed_functions += 1,
                SqlObjectEnum::View(_) => self.new_iter.completed_views += 1,
            }
            self.new_iter.completed_objects.push(other.name());
            Some(DbCompareResult::Alter {
                old: obj,
                new: other,
            })
        } else {
            Some(DbCompareResult::Drop(obj))
        }
    }
}

/// Write `CREATE` statements to the file specified by the object type and name
pub async fn write_create_statements_to_file<S, P>(
    object: &S,
    root_directory: P,
) -> Result<(), PgDiffError>
where
    S: SqlObject,
    P: AsRef<Path>,
{
    let mut statements = String::new();
    object.create_statements(&mut statements)?;

    let path = root_directory
        .as_ref()
        .join(object.object_type_name().to_lowercase());
    tokio::fs::create_dir_all(&path).await?;
    let mut file = File::create(path.join(format!("{}.pgsql", object.name()))).await?;
    file.write_all(statements.as_bytes()).await?;
    Ok(())
}

/// Append the `CREATE` statements to the owning table's file
pub async fn append_create_statements_to_owner_table_file<S, P>(
    object: &S,
    owner_table: &SchemaQualifiedName,
    root_directory: P,
) -> Result<(), PgDiffError>
where
    S: SqlObject,
    P: AsRef<Path>,
{
    let mut statements = String::new();
    object.create_statements(&mut statements)?;

    let path = root_directory.as_ref().join("table");
    tokio::fs::create_dir_all(&path).await?;
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.join(format!("{}.pgsql", owner_table)))
        .await?;
    file.write_all("\n".as_bytes()).await?;
    file.write_all(statements.as_bytes()).await?;
    Ok(())
}

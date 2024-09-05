use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter, Write};
use std::path::Path;

use async_walkdir::WalkDir;
use futures::stream::StreamExt;
use pg_query::protobuf::{ConstrType, node::Node, RangeVar};
use serde::Deserialize;
use sqlx::{Error, PgPool, query_as, query_scalar};
use sqlx::postgres::PgDatabaseError;
use sqlx::postgres::types::Oid;
use sqlx::types::Uuid;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::object::{BUILT_IN_FUNCTIONS, BUILT_IN_NAMES, compare_object_groups, Constraint, Extension, find_index, Function, get_constraints, get_extensions, get_functions, get_indexes, get_schemas, get_sequences, get_tables, get_triggers, get_udts, get_views, Index, Schema, SchemaQualifiedName, Sequence, SqlObject, Table, Trigger, Udt, View};
use crate::object::plpgsql::parse_plpgsql_function;
use crate::object::policy::{get_policies, Policy};
use crate::PgDiffError;

pub struct DatabaseMigration {
    pool: PgPool,
    database: Database,
    source_control_database: SourceControlDatabase,
}

impl DatabaseMigration {
    pub async fn new<P>(pool: PgPool, source_control_directory: P) -> Result<Self, PgDiffError>
    where
        P: AsRef<Path>,
    {
        let database = Database::from_connection(pool.clone()).await?;
        let source_control_database =
            SourceControlDatabase::from_directory(pool.clone(), source_control_directory).await?;
        Ok(Self {
            pool,
            database,
            source_control_database,
        })
    }
    
    pub async fn plan_migration(&mut self) -> Result<String, PgDiffError> {
        self.source_control_database.apply_to_temp_database().await?;
        let source_control_temp_database = self.source_control_database.scrape_temp_database().await?;
        let migration_script = self.database.compare_to_other_database(&source_control_temp_database)?;
        Ok(migration_script)
    }
}

impl Drop for DatabaseMigration {
    fn drop(&mut self) {
        let db_name = self.source_control_database.temp_db_name.clone();
        let pool = self.pool.clone();
        let fut = async move {
            sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE);", db_name))
                .execute(&pool)
                .await
                .unwrap_or_else(|_| panic!("Failed to drop temp database named '{}'", db_name));
        };
        tokio::spawn(fut);
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
                println!(
                    "Skipping SQL code block since the source text could not be parsed. {error}\n{}",
                    code
                )
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
                            println!("Skipping plpg/sq; code block since the source text could not be parsed for objects. {error}")
                        }
                    }
                }
            }
            Err(error) => {
                println!("Skipping plpg/sql code block since the source text could not be parsed. {error}\n{}", code)
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
                                println!("Could not deparse plpg/sql function. {error}");
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
                            println!(
                                "Unknown language '{}' for function. Could not parse.",
                                language.sval
                            )
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
                    println!(
                        "Skipping code block since the language is not supported. Lang ID = {}",
                        inline_code_block.lang_oid
                    )
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
                        Err(error) => println!("Error trying to deparse view query. {error}"),
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
    fn new<S>(statement: &str, name: S) -> Self
    where
        S: Into<SchemaQualifiedName>,
    {
        Self {
            statement: statement.to_owned(),
            object: name.into(),
            dependencies: vec![],
        }
    }

    fn add_dependency(&mut self, name: SchemaQualifiedName) {
        self.dependencies.push(name);
    }

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
            if let Some(index) = find_index(&self.statements, |s| s.has_dependencies_met(&self.completed_objects)) {
                let statement = self.statements.remove(index);
                self.completed_objects.insert(statement.object.clone());
                return Some(statement);
            }
            if let Some(index) = find_index(&self.statements, |s| self.statements.iter().all(|other| !s.depends_on(&other.object))) {
                let statement = self.statements.remove(index);
                self.completed_objects.insert(statement.object.clone());
                return Some(statement);
            }
            return Some(self.statements.remove(0))
        }
        
        if self.initial_failed_count == 0 {
            self.initial_failed_count = self.failed_statements.len();
            return Some(self.failed_statements.remove(self.failed_statement_index));
        }
        
        self.failed_statement_index += 1;
        self.failed_statement_index = self.failed_statement_index.clamp(0, self.failed_statements.len() - 1);
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
    pool: PgPool,
    statements: Vec<DdlStatement>,
}

impl SourceControlDatabase {
    fn new(pool: PgPool) -> Self {
        Self {
            temp_db_name: format!(
                "pg_diff_rs_{}",
                Uuid::new_v4().to_string().replace("-", "_")
            ),
            pool,
            statements: vec![],
        }
    }

    pub async fn from_directory<P>(pool: PgPool, files_path: P) -> Result<Self, PgDiffError>
    where
        P: AsRef<Path>,
    {
        let mut builder = SourceControlDatabase::new(pool);
        let mut entries = WalkDir::new(files_path).map(|entry| entry.map(|e| e.path()));
        while let Some(result) = entries.next().await {
            let path = result?;
            if path.is_dir() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|f| f.to_str()) else {
                println!("Skipping {:?}", path);
                continue;
            };
            if !file_name.ends_with(".pgsql") && !file_name.ends_with(".sql") {
                println!("Skipping {:?}", file_name);
                continue;
            }
            builder.append_source_file(path).await?;
        }
        Ok(builder)
    }

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
            let root_node = extract_option(
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
                    SchemaQualifiedName::from_schema_name(&create_schema.schemaname)
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
            let mut statement = DdlStatement::new(query, parent_object);
            for item in NodeIter::new(root_node) {
                statement.add_dependency(item.clone());
            }

            self.statements.push(statement);
        }

        Ok(())
    }

    pub async fn apply_to_temp_database(&mut self) -> Result<(), PgDiffError> {
        let query = include_str!("./../../queries/check_create_db_role.pgsql");
        let can_create_database: bool = query_scalar(query).fetch_one(&self.pool).await?;
        if !can_create_database {
            return Err(PgDiffError::General("Current user does not have permission to create a temp database for migration staging".to_string()));
        }

        let db_options = DatabaseOptions::from_connection(&self.pool).await?;
        let create_database = format!("CREATE DATABASE {}{};", self.temp_db_name, db_options);
        sqlx::query(&create_database).execute(&self.pool).await?;
        println!("Created temp database: {}", self.temp_db_name);
        let db_options = (*self.pool.connect_options())
            .clone()
            .database(&self.temp_db_name);
        self.pool = PgPool::connect_with(db_options).await?;

        let mut iter = StatementIter::new(&self.statements);
        let mut i = 0;
        while let Some(statement) = iter.next() {
            if let Err(error) = sqlx::query(&statement.statement).execute(&self.pool).await {
                match &error {
                    Error::Database(db_error) => {
                        let pg_error = db_error.downcast_ref::<PgDatabaseError>();
                        let message = pg_error.message();
                        let Some(item) = self.statements.iter_mut().find(|s| **s == statement) else {
                            return Err(error.into());
                        };
                        if message.ends_with(" does not exist") {
                            let name: String = message.chars().skip_while(|c| *c == '"').take_while(|c| *c == '"').collect();
                            let dependency = SchemaQualifiedName::from(name.trim_matches('"'));
                            item.dependencies.push(dependency);
                        }
                        iter.add_back_failed_statement(item.clone());
                        continue;
                    }
                    _ => return Err(error.into()),
                }
            }
            i += 1;
            println!("Statement {}/{}\n", i, self.statements.len());
        }
        if iter.has_remaining() {
            let remaining_statements = iter.take_remaining().into_iter().map(|s| s.statement).collect();
            return Err(PgDiffError::SourceControlScript { remaining_statements });
        }
        println!("Done applying source control DDL statements to database");
        Ok(())
    }
    
    async fn scrape_temp_database(&self) -> Result<Database, PgDiffError> {
        Database::from_connection(self.pool.clone()).await
    }
}

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

fn extract_string(node: &pg_query::protobuf::Node) -> Option<&String> {
    match &node.node {
        Some(pg_query::NodeEnum::String(pg_query::protobuf::String { sval, .. })) => Some(sval),
        _ => None,
    }
}

fn extract_option<P, I>(path: P, option: &Option<I>, message: String) -> Result<&I, PgDiffError>
where
    P: AsRef<Path>,
{
    option.as_ref().ok_or(PgDiffError::FileQueryParse {
        path: path.as_ref().into(),
        message,
    })
}

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalProvider::Libc {
                lc_collate,
                lc_ctype,
            } => {
                write!(
                    f,
                    "\n\tLOCALE_PROVIDER 'libc'\n\tLC_COLLATE '{}'\n\tLC_CTYPE '{}'",
                    lc_collate, lc_ctype
                )
            }
            LocalProvider::Icu {
                icu_locale,
                icu_rules,
            } => {
                write!(
                    f,
                    "\n\tLOCALE_PROVIDER 'icu'\n\tICU_LOCALE '{}'",
                    icu_locale
                )?;
                if let Some(icu_rules) = icu_rules {
                    write!(f, "\n\tICU_RULES '{}'", icu_rules)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DatabaseOptions {
    encoding: String,
    locale: Option<String>,
    #[sqlx(json)]
    locale_provider: LocalProvider,
    collation_version: String,
}

impl Display for DatabaseOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " WITH\n\tENCODING '{}'\n\tCOLLATION_VERSION '{}'",
            self.encoding, self.collation_version
        )?;
        if let Some(locale) = &self.locale {
            write!(f, "\n\tLOCALE '{}'", locale)?;
        }
        write!(f, "{}", self.locale_provider)
    }
}

impl DatabaseOptions {
    async fn from_connection(pool: &PgPool) -> Result<Self, PgDiffError> {
        let query = include_str!("./../../queries/database.pgsql");
        let db_options = query_as(query).fetch_one(pool).await?;
        Ok(db_options)
    }
}

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
    pub async fn from_connection(pool: PgPool) -> Result<Self, PgDiffError> {
        let mut schemas = get_schemas(&pool).await?;
        let schema_names: Vec<&str> = schemas
            .iter()
            .map(|s| s.name.schema_name.as_str())
            .collect();
        let udts = get_udts(&pool, &schema_names).await?;
        let tables = get_tables(&pool, &schema_names).await?;
        let table_oids: Vec<Oid> = tables.iter().map(|t| t.oid).collect();
        let policies = get_policies(&pool, &table_oids).await?;
        let constraints = get_constraints(&pool, &table_oids).await?;
        let indexes = get_indexes(&pool, &table_oids).await?;
        let triggers = get_triggers(&pool, &table_oids).await?;
        let sequences = get_sequences(&pool, &schema_names).await?;
        let functions = get_functions(&pool, &schema_names).await?;
        let views = get_views(&pool, &schema_names).await?;
        if let Some(index) = find_index(&schemas, |schema| schema.name.schema_name == "public")
        {
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
            extensions: get_extensions(&pool).await?,
        };
        database.collect_additional_dependencies(&pool).await?;
        Ok(database)
    }

    async fn collect_additional_dependencies(&mut self, pool: &PgPool) -> Result<(), PgDiffError> {
        for function in self.functions.iter_mut() {
            function.extract_more_dependencies(pool).await?;
        }
        Ok(())
    }

    pub async fn script_out<P>(&self, output_path: P) -> Result<(), PgDiffError>
    where
        P: AsRef<Path>,
    {
        for schema in &self.schemas {
            write_create_statements_to_file(schema, &output_path).await?;
        }
        for udt in &self.udts {
            write_create_statements_to_file(udt, &output_path).await?;
        }
        for table in &self.tables {
            write_create_statements_to_file(table, &output_path).await?;
            for policy in self.policies.iter().filter(|c| c.table_oid == table.oid) {
                append_create_statements_to_owner_table_file(
                    policy,
                    &policy.owner_table_name,
                    &output_path,
                )
                .await?
            }
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
        for view in &self.views {
            write_create_statements_to_file(view, &output_path).await?;
        }
        for extension in &self.extensions {
            write_create_statements_to_file(extension, &output_path).await?;
        }
        Ok(())
    }
    
    fn compare_to_other_database(&self, other: &Self) -> Result<String, PgDiffError> {
        let mut result = String::new();
        compare_object_groups(&self.schemas, &other.schemas, &mut result)?;
        compare_object_groups(&self.extensions, &other.extensions, &mut result)?;
        compare_object_groups(&self.udts, &other.udts, &mut result)?;
        compare_object_groups(&self.tables, &other.tables, &mut result)?;
        compare_object_groups(&self.constraints, &other.constraints, &mut result)?;
        compare_object_groups(&self.indexes, &other.indexes, &mut result)?;
        compare_object_groups(&self.triggers, &other.triggers, &mut result)?;
        compare_object_groups(&self.policies, &other.policies, &mut result)?;
        compare_object_groups(&self.views, &other.views, &mut result)?;
        compare_object_groups(&self.functions, &other.functions, &mut result)?;
        compare_object_groups(&self.sequences, &other.sequences, &mut result)?;
        Ok(result)
    }
}

/// Write create statements to file
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
    writeln!(&mut statements, "\n-- {:?}", object.dependencies())?;

    let path = root_directory
        .as_ref()
        .join(object.object_type_name().to_lowercase());
    tokio::fs::create_dir_all(&path).await?;
    let mut file = File::create(path.join(format!("{}.pgsql", object.name()))).await?;
    file.write_all(statements.as_bytes()).await?;
    Ok(())
}

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
    writeln!(&mut statements, "\n-- {:?}", object.dependencies())?;

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

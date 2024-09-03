use std::collections::{HashSet, VecDeque};
use std::fmt::Write;
use std::path::Path;

use async_walkdir::WalkDir;
use futures::stream::StreamExt;
use pg_query::protobuf::{node::Node, ConstrType, RangeVar, SetOperation};
use sqlx::postgres::types::Oid;
use sqlx::PgPool;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::object::plpgsql::parse_plpgsql_function;
use crate::object::policy::{get_policies, Policy};
use crate::object::{
    get_constraints, get_extensions, get_functions, get_indexes, get_schemas, get_sequences,
    get_tables, get_triggers, get_udts, get_views, Constraint, Extension, Function, Index, Schema,
    SchemaQualifiedName, Sequence, SqlObject, Table, Trigger, Udt, View,
};
use crate::PgDiffError;

const BUILT_IN_NAMES: &[&str] = &[
    "array_agg",
    "json_object",
    "json_agg",
    "text",
    "oid",
    "inet",
    "jsonb",
    "char",
    "uuid",
    "array_length",
    "date",
];

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
        iter.extract_objects_from_node();
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

    fn parse_inline_code(&mut self, code: &str) {
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
                    "Skipping code block since the source text could not be parsed. {error}\n{}",
                    code
                )
            }
        }
    }

    fn process_select_statement(&mut self, select_statement: &'n pg_query::protobuf::SelectStmt) {
        self.queue_nodes(&select_statement.target_list);
        self.queue_node(&select_statement.where_clause);
        if let Some(with_clause) = &select_statement.with_clause {
            self.process_with_clause(with_clause);
        }
        self.queue_nodes(&select_statement.values_lists);
        match select_statement.op() {
            SetOperation::SetopNone => {
                self.queue_nodes(&select_statement.from_clause);
            }
            SetOperation::SetopUnion | SetOperation::SetopExcept | SetOperation::SetopIntersect => {
                if let Some(left) = &select_statement.larg {
                    self.process_select_statement(left);
                }
                if let Some(right) = &select_statement.rarg {
                    self.process_select_statement(right);
                }
            }
            _ => (),
        }
    }

    fn process_with_clause(&mut self, with_clause: &'n pg_query::protobuf::WithClause) {
        self.queue_nodes(&with_clause.ctes);
    }

    fn extract_objects_from_node(&mut self) -> bool {
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
                self.queue_nodes(&create_function.options);
            }
            Node::DefElem(def_element) => {
                if def_element.defname == "as" {
                    self.queue_node(&def_element.arg);
                }
            }
            Node::List(list) => {
                self.queue_nodes(&list.items);
            }
            Node::String(string) => {
                self.parse_inline_code(&string.sval);
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
                14 => self.parse_inline_code(&inline_code_block.source_text),
                13545 => match parse_plpgsql_function(&inline_code_block.source_text) {
                    Ok(functions) => {
                        for function in functions {
                            match function.get_objects() {
                                Ok(objects) => {
                                    self.queued_elements.append(&mut VecDeque::from(objects));
                                }
                                Err(error) => {
                                    println!("Skipping code block since the source text could not be parsed for objects. {error}\n{}", inline_code_block.source_text)
                                }
                            }
                        }
                    }
                    Err(error) => {
                        println!("Skipping code block since the source text could not be parsed. {error}\n{}", inline_code_block.source_text)
                    }
                },
                _ => {
                    println!(
                        "Skipping code block since the language is not supported. Lang ID = {}",
                        inline_code_block.lang_oid
                    )
                }
            },
            Node::CallStmt(call_statement) => {
                if let Some(func_call) = &call_statement.funccall {
                    self.queue_nodes(&func_call.args);
                    self.queue_names(&func_call.funcname);
                }
            }
            Node::AlterTypeStmt(alter_type) => {
                self.queue_nodes(&alter_type.options);
            }
            Node::CompositeTypeStmt(composite_type) => {
                self.queue_nodes(&composite_type.coldeflist);
            }
            Node::SelectStmt(select) => {
                self.process_select_statement(select);
            }
            Node::SubLink(sub_link) => {
                self.queue_node(&sub_link.subselect);
                self.queue_node(&sub_link.testexpr);
                self.queue_node(&sub_link.xpr);
            }
            Node::ResTarget(result_target) => {
                self.queue_node(&result_target.val);
                self.queue_nodes(&result_target.indirection);
            }
            Node::JoinExpr(join_expression) => {
                self.queue_node(&join_expression.larg);
                self.queue_node(&join_expression.rarg);
            }
            Node::RangeSubselect(range_sub_select) => {
                self.queue_node(&range_sub_select.subquery);
            }
            Node::InsertStmt(insert_statement) => {
                self.queue_relation(&insert_statement.relation);
                self.queue_node(&insert_statement.select_stmt);
                if let Some(with_clause) = &insert_statement.with_clause {
                    self.process_with_clause(with_clause);
                }
            }
            Node::WithClause(with_clause) => {
                self.process_with_clause(with_clause);
            }
            Node::CommonTableExpr(cte) => {
                self.queue_node(&cte.ctequery);
            }
            Node::UpdateStmt(update_statement) => {
                self.queue_relation(&update_statement.relation);
                self.queue_nodes(&update_statement.target_list);
                self.queue_node(&update_statement.where_clause);
                if let Some(with_clause) = &update_statement.with_clause {
                    self.process_with_clause(with_clause);
                }
                self.queue_nodes(&update_statement.from_clause);
            }
            Node::DeleteStmt(delete_statement) => {
                self.queue_relation(&delete_statement.relation);
                self.queue_node(&delete_statement.where_clause);
                if let Some(with_clause) = &delete_statement.with_clause {
                    self.process_with_clause(with_clause);
                }
                self.queue_nodes(&delete_statement.using_clause);
            }
            Node::ViewStmt(view) => {
                self.queue_node(&view.query);
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
        if self.extract_objects_from_node() {
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

#[derive(Debug)]
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
}

#[derive(Debug)]
pub struct SourceControlDatabase {
    statements: Vec<DdlStatement>,
    non_ddl_statements: Vec<String>,
    unresolved_objects: HashSet<SchemaQualifiedName>,
}

impl SourceControlDatabase {
    fn new() -> Self {
        Self {
            statements: vec![],
            non_ddl_statements: vec![],
            unresolved_objects: HashSet::new(),
        }
    }

    pub async fn from_directory<P>(files_path: P) -> Result<Self, PgDiffError>
    where
        P: AsRef<Path>,
    {
        let mut builder = SourceControlDatabase::new();
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
                self.unresolved_objects.insert(item);
            }

            self.statements.push(statement);
        }

        for statement in &self.statements {
            self.unresolved_objects.remove(&statement.object);
        }

        Ok(())
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
            if BUILT_IN_NAMES.contains(&local_name.as_str()) {
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
    pub async fn from_connection(pool: &PgPool) -> Result<Self, PgDiffError> {
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
        if let Some(index) = schemas
            .iter()
            .enumerate()
            .find(|(_, schema)| schema.name.schema_name == "public")
            .map(|(i, _)| i)
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
            extensions: get_extensions(pool).await?,
        };
        database.collect_additional_dependencies(pool).await?;
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

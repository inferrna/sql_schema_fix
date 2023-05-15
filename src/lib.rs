
fn match_defaults(a: &str, b: &str) -> bool {
    a.eq(b)
        || matches!((a, b), ("on update current_timestamp()", "on update CURRENT_TIMESTAMP") | ("current_timestamp()", "CURRENT_TIMESTAMP"))
        || matches!((b, a), ("on update current_timestamp()", "on update CURRENT_TIMESTAMP") | ("current_timestamp()", "CURRENT_TIMESTAMP"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableIndex {
    columns: Vec<String>,
    unique: bool,
    primary: bool
}

impl TableIndex {
    pub fn new(columns_string: String, unique: bool, primary: bool) -> Self {
        let mut columns: Vec<String> = columns_string
            .split(',')
            .map(|c|c.trim().to_string())
            .collect();
        columns.sort();
        Self {
            columns,
            unique,
            primary
        }
    }
    pub fn columns(&self) -> String {
        self.columns.join(",")
    }
    pub fn unique(&self) -> bool {
        self.unique
    }
    pub fn primary(&self) -> bool {
        self.primary
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    datatype: String,
    is_numeric: bool,
    is_text: bool,
    default: String,
    nullable: bool,
    extra: Option<String>,
}

impl Column {
    pub fn new(datatype: String, is_numeric: bool, default: Option<String>, nullable: bool, extra: Option<String>) -> Self {
        let is_text = datatype.contains("char") || datatype.contains("text");
        Self {
            datatype,
            is_numeric,
            is_text,
            default: default.unwrap_or("NULL".to_string())
                .trim_matches('"')
                .trim_matches('\'').to_string(),
            nullable,
            extra
        }
    }
    fn cmp_extra(&self, other: &Self) -> bool {
        match (self.extra.as_ref(), other.extra.as_ref()) {
            (None, Some(_)) => false,
            (Some(_), None) => false,
            (Some(a), Some(b)) => match_defaults(a.as_str(), b.as_str()),
            (None, None) => true,
        }
    }
    fn cmp_defaults(&self, other: &Self) -> bool {
        match (self.get_default(), other.get_default()) {
            (Some(a), Some(b)) =>  match_defaults(a.as_str(), b.as_str()),
            (None, None) => true,
            _ => false
        }
    }
    fn get_nullable_str(&self) -> &str {
        if self.nullable { "" } else { " NOT NULL" }
    }
    pub fn get_extra_string(&self) -> String {
        self.extra.as_ref().map(|x| format!(" {x}")).unwrap_or("".to_string())
    }
    pub fn generate_constructor(&self) -> String {
        format!("{dtype}{nullable}{default}{extra}",
                dtype=self.datatype,
                default=self.get_default().map(|d| format!(" DEFAULT {d}")).unwrap_or("".to_string()),
                nullable = self.get_nullable_str(),
                extra=self.get_extra_string()
        )
    }
    pub fn generate_create_query(&self, name: &str, table_name: &str) -> String {
        format!("ALTER TABLE {table_name} ADD COLUMN {name} {constructor};", constructor = self.generate_constructor())
    }
    pub fn generate_change_query(&self, name: &str, table_name: &str) -> String {
        format!("ALTER TABLE {table_name} MODIFY {name} {constructor};", constructor = self.generate_constructor())
    }
    pub fn get_default(&self) -> Option<String> {
        let raw_def_val = match (self.nullable, self.default.as_str()) {
            (false,  "NULL") => None,
            _ => match self.is_text {
                false => Some(self.default.clone()),
                true => Some(format!("\"{}\"", self.default)),
            }
        }?;
        let def_val = match (self.is_text, self.default.as_str()) {
            (_, "NULL") => "NULL".to_string(),
            (true, _) => format!("\'{raw_def_val}\'"),
            (false, _) => raw_def_val
        };
        Some(def_val)
    }
    pub fn datatype(&self) -> &str {
        &self.datatype
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn extra(&self) -> Option<&String> {
        self.extra.as_ref()
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.datatype.eq(&other.datatype)
            && self.cmp_extra(other)
            && self.cmp_defaults(other)
    }
}
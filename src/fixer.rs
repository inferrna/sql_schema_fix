use mysql::{Params, PooledConn};
use indexmap::IndexMap;
use mysql::prelude::Queryable;
use sql_schema_diff::{Column, TableIndex};
use crate::KeysOrder;

fn extract_columns_from_db(conn: &mut PooledConn, db_name: &str, table_name: &str) -> IndexMap<String, Column> {
    let q = format!("SELECT column_name,column_type,NUMERIC_PRECISION IS NOT NULL,column_default,is_nullable=\"YES\",extra \
        FROM information_schema.columns WHERE table_schema='{db_name}' AND table_name='{table_name}'\
        ORDER BY ordinal_position");
    conn.query_map(q, |(name, datatype, is_numeric, default, nullable, extra )|(name, Column::new(datatype, is_numeric, default, nullable, extra)))
        .unwrap()
        .into_iter()
        .collect()
}

fn extract_indexes(conn: &mut PooledConn, db_name: &str, table_name: &str) -> IndexMap<String, TableIndex> {
    let q = format!("SELECT INDEX_NAME,GROUP_CONCAT(COLUMN_NAME),NOT(NON_UNIQUE),INDEX_NAME=\"PRIMARY\" \
    FROM information_schema.statistics WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{db_name}' \
    GROUP BY INDEX_NAME,NON_UNIQUE");
    //eprintln!("{}", q.as_str());
    conn.query_map(q, |(name, columns, unique, primary)| (name, TableIndex::new(columns, unique, primary)))
        .unwrap()
        .into_iter()
        .collect()
}

fn intersect_keys<T>(a: &IndexMap<String, T>, b: &IndexMap<String, T>) -> Vec<String> {
    a.keys()
        .filter(|&k|!b.contains_key(k))
        .cloned()
        .collect()
}

pub(crate) struct MySQLFixer {
    master_conn: PooledConn,
    slave_conn: PooledConn,
    master_db: String,
    slave_db: String,
    show_plus: bool,
    show_minus: bool
}

impl MySQLFixer {
    pub fn new(master_conn: PooledConn, slave_conn: PooledConn, master_db: String, slave_db: String, show_plus: bool, show_minus: bool) -> Self {
        Self { master_conn, slave_conn, master_db, slave_db, show_plus, show_minus }
    }
    fn execute_slave(&mut self, q: String) {
        self.slave_conn.exec::<String, _, _, >(q.clone(), Params::Empty)
            .map_err(|e| {
                println!("/* Failed with {} */\n{q}", e);
            }).ok();
    }
    fn compare_column_values(&mut self, table_name: &str) {
        let master_columns = extract_columns_from_db(&mut self.master_conn, &self.master_db, table_name);
        let slave_columns = extract_columns_from_db(&mut self.slave_conn, &self.slave_db, table_name);
        for (column_name, master_column) in master_columns.iter() {
            if let Some(slave_column) = slave_columns.get(column_name) {
                if !master_column.eq(slave_column) {
                    //dbg!(&master_column,&slave_column);
                    self.execute_slave(format!("ALTER TABLE {table_name} MODIFY {column_name} {};",
                             master_column.generate_constructor()));
                }
            }
        }
    }

    fn compare_indexes_values(&mut self, master_indexes: &IndexMap<String, TableIndex>, slave_indexes: &IndexMap<String, TableIndex>, slave_table: &str) {
        for (index_name, master_index) in master_indexes.iter() {
            if let Some(slave_index) = slave_indexes.get(index_name) {
                if !master_index.eq(slave_index) {
                    dbg!(&master_index,&slave_index);
                    if master_index.primary() {
                        self.execute_slave(format!("ALTER TABLE {slave_table} DROP PRIMARY KEY, ADD PRIMARY KEY({});", master_index.columns()));
                    } else {
                        self.execute_slave(format!("ALTER TABLE {slave_table} DROP INDEX {index_name}, ADD INDEX {index_name}({columns});",
                                 columns = master_index.columns()));
                    }
                }
            }
        }
    }

    fn compare_columns_order(&mut self, table_name: &str) {
        let master_columns = extract_columns_from_db(&mut self.master_conn, &self.master_db, table_name);
        let slave_columns = extract_columns_from_db(&mut self.slave_conn, &self.slave_db, table_name);
        let master_order= master_columns.get_keys_order();
        let slave_order = slave_columns.get_keys_order();
        if !master_order.eq(&slave_order) {
            let modifiers: Vec<String> = master_order
                .into_iter()
                .filter_map(|(prev_col, next_col)| {
                    let col = slave_columns.get(&prev_col)?;
                    let q = format!("MODIFY {prev_col} {constructor} AFTER {next_col}", constructor=col.generate_constructor());
                    Some(q)
                })
                .collect();
            self.execute_slave(format!("ALTER TABLE {table_name} {m};", m=modifiers.join(", ")));
        }
    }
    fn compare_columns(&mut self, table_name: &str) {
        let master_columns = extract_columns_from_db(&mut self.master_conn, &self.master_db, table_name);
        let slave_columns = extract_columns_from_db(&mut self.slave_conn, &self.slave_db, table_name);

        let cols2add = if self.show_plus { intersect_keys(&master_columns, &slave_columns) } else { vec![] };
        let cols2drop = if self.show_minus { intersect_keys(&slave_columns, &master_columns) } else { vec![] };

        for col_name in cols2add.iter() {
            if let Some(mc) = master_columns.get(col_name) {
                let table_name = format!("{}.{table_name}", &self.slave_db);
                self.execute_slave(mc.generate_create_query(col_name.as_str(), table_name.as_str()));
            }
        }
        for col_name in cols2drop.iter() {
            let table_name = format!("{}.{table_name}", &self.slave_db);
            self.execute_slave(format!("ALTER TABLE {table_name} DROP COLUMN {col_name};"));
        }
    }

    fn compare_indexes(&mut self, table_name: &str) {
        let master_indexes = extract_indexes(&mut self.master_conn, &self.master_db, table_name);
        let slave_indexes = extract_indexes(&mut self.slave_conn, &self.slave_db, table_name);
        let idxs2add = if self.show_plus { intersect_keys(&master_indexes, &slave_indexes) } else { vec![] };
        let idxs2drop = if self.show_minus { intersect_keys(&slave_indexes, &master_indexes) } else { vec![] };

        for idx_name in idxs2add.iter() {
            if let Some(index) = master_indexes.get(idx_name) {
                if index.primary() {
                    self.execute_slave(format!("ALTER TABLE {slave_db}.{table_name} ADD CONSTRAINT PRIMARY KEY ({columns});",
                                               slave_db = &self.slave_db, columns = index.columns()));
                } else {
                    let uniq_part = if index.unique() { " UNIQUE" } else { " " };
                    self.execute_slave(format!("CREATE{uniq_part} INDEX {idx_name} ON {slave_db}.{table_name}({columns});",
                                               slave_db = &self.slave_db, columns = index.columns()));
                }
            }
        }
        for idx_name in idxs2drop.iter() {
            if let Some(index) = master_indexes.get(idx_name) {
                if index.primary() {
                    self.execute_slave(format!("ALTER TABLE {slave_db}.{table_name} DROP PRIMARY KEY;", slave_db = &self.slave_db));
                } else {
                    self.execute_slave(format!("ALTER TABLE {slave_db}.{table_name} DROP INDEX {idx_name};", slave_db = &self.slave_db));
                }
            }
        }
        self.compare_indexes_values(&master_indexes, &slave_indexes, table_name);
    }

    pub(crate) fn compare_tables(&mut self,) {
        let master_tables: Vec<String> = self.master_conn.query::<String, String>(format!("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='{master_db}'", master_db = &self.master_db)).unwrap();
        let slave_tables: Vec<String> = self.slave_conn.query::<String, String>(format!("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='{slave_db}'", slave_db = &self.slave_db)).unwrap();
        if self.show_plus {
            for master_table in master_tables.iter() {
                if !slave_tables.contains(master_table) {
                    let new_table_q = self.master_conn
                        .query_first::<(String, String), String>(format!("SHOW CREATE TABLE {}.{master_table}", &self.master_db))
                        .unwrap()
                        .unwrap().1
                        .replace(['\n', '\r'], "");
                    self.execute_slave(format!("{new_table_q};"));
                }
            }
        }
        for table_name in slave_tables.iter() {
            if !master_tables.contains(table_name) {
                if self.show_minus {
                    self.execute_slave(format!("DROP TABLE {}.{table_name};", &self.slave_db));
                }
            } else {
                self.compare_columns(table_name);
                self.compare_column_values(table_name);
                self.compare_columns_order(table_name);
                self.compare_indexes(table_name);
            }
        }
    }
}

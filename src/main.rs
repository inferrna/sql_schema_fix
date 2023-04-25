use std::path::PathBuf;
use mysql::*;
use mysql::prelude::*;
use clap::{Parser, ValueEnum};
use indexmap::IndexMap;
use sql_schema_fix::{Column, TableIndex};
use crate::fixer::MySQLFixer;

mod fixer;

fn get_mysql_connection(dburl: &str) -> (PooledConn, String) {
    let mysql_opts = Opts::from_url(dburl).unwrap();
    let db_name = mysql_opts.get_db_name().expect("Should provide database name").to_string();
    let pool = Pool::new(mysql_opts).unwrap();
    (pool.get_conn().unwrap(), db_name)
}

#[derive(Clone, ValueEnum, Parser, Debug)]
enum WhatToShow {
    ///Apply only positive changes. Add table, column, etc.
    Plus,
    ///Apply only negative changes. Drop table, column, etc.
    Minus,
    ///Apply all changes.
    All
}

impl WhatToShow {
    fn show_plus_minus(&self) -> (bool, bool) {
        match self {
            WhatToShow::Plus => (true, false),
            WhatToShow::Minus => (false, true),
            WhatToShow::All => (true, true)
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Parameters {
    ///Master DB url. Example: "mysql://user:password@remotehost:3307/db_name"
    #[arg(long)]
    master_db: String,
    ///Slave DB url. Example: "mysql://user:password@localhost:3307/db_name"
    #[arg(long)]
    slave_db: String,
    ///Changes to show
    #[arg(long)]
    what_to_show: WhatToShow
}

impl Parameters {
    fn get_fixer(&self) -> MySQLFixer {
        let (master_conn, master_db) = get_mysql_connection(&self.master_db);
        let (slave_conn, slave_db) = get_mysql_connection(&self.slave_db);
        let (show_plus, show_minus) = self.what_to_show.show_plus_minus();
        MySQLFixer::new (master_conn, slave_conn,
            master_db, slave_db,
            show_plus, show_minus)
    }
}

trait KeysOrder<K,V> {
    fn get_keys_order(&self) -> Vec<(K, K)>;
}

impl<K: Clone,V> KeysOrder<K,V> for IndexMap<K,V> {
    fn get_keys_order(&self) -> Vec<(K, K)> {
        let mut key2follow = self.keys().take(1).last().unwrap();
        self.keys()
            .skip(1)
            .map(|k|{
                let res = (k.clone(), key2follow.clone());
                key2follow = k;
                res
            }).collect()
    }
}

fn main() {
    let params = Parameters::parse();
    let mut fixer = params.get_fixer();
    fixer.compare_tables();
}

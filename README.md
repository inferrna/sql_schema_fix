# sql_schema_fix

Usage: sql_schema_fix --master-db <MASTER_DB> --slave-db <SLAVE_DB> --what-to-show <WHAT_TO_SHOW>

Options:


  --master-db <MASTER_DB>
          Master DB url. Example: "mysql://user:password@remotehost:3307/db_name"

  --slave-db <SLAVE_DB>
          Slave DB url. Example: "mysql://user:password@localhost:3307/db_name"
          
  --what-to-show <WHAT_TO_SHOW>
          Changes to show
          
  Possible values:
  
   - plus:  Apply only positive changes. Add table, column, etc
          
   - minus: Apply only negative changes. Drop table, column, etc
          
   - all:   Apply all changes

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version

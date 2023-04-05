from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 sql_statement = '',    # SQL statement imported from SqlQueries class
                 table = '',            # Query result is saved in this table
                 append_only = False,   # Switching between append_only and delete_insert mode
                 redshift_conn_id = '', # Redshift connection
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.table = table
        self.append_only = append_only
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # if append_only is False, the data will be deleted before inserting new data
        if self.append_only == False:
            self.log.info(f'Delete all records of dimension table - {self.table}')
            redshift_hook.run(f"TRUNCATE {self.table}")
        
        # if append_only is True, old data will not be deleted
        self.log.info(f'Transform staging data and insert query result into dimension table - {self.table}')
        results = redshift_hook.get_records(self.sql_statement)

        # Insert data to dimension table
        redshift_hook.insert_rows(f'{self.table}', results)
        self.log.info(f'Successfully insert data into dimension table - {self.table}.')

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_statement = '',    # SQL statement imported from SqlQueries class
                 table = '',            # Query result is saved in this table
                 redshift_conn_id = '', # Redshift connection
                 *args, **kwargs):
                 
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Get records from transformation
        self.log.info(f'Transform staging data and insert query result into fact table - {self.table}')
        results = redshift_hook.get_records(self.sql_statement)

        # Insert data to fact table
        redshift_hook.insert_rows(f'{self.table}', results)
        self.log.info(f'Successfully insert data into fact table - {self.table}.')

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 checks = [],
                 redshift_conn_id = '',
                 *args, **kwargs):
                 

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Start data quality check')

        for i, dq_check in enumerate(self.checks):
            records = redshift_hook.get_records(dq_check['test_sql'])
            num_records = records[0][0]

            if dq_check['comparison'] == '=':
                if num_records > dq_check['expected_result']:
                    raise ValueError(f'Data quality check #{i} failed because the considered column has NULL values.')
            if dq_check['comparison'] == '>':
                if num_records == dq_check['expected_result']:
                    raise ValueError(f'Data quality check #{i} failed because the table has no rows.')
            self.log.info(f'Data quality check #{i} passed')
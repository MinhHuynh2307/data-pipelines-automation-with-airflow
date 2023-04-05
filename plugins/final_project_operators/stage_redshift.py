from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {} 
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        COMPUPDATE OFF 
        REGION '{}' 
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 table = '',            # staging table name
                 s3_bucket = '',        # S3 bucket, from which data is copied to redshift
                 s3_key = '',           # Ex: log-data or song-data
                 aws_iam_role = '',     # arn of IAM role, which grants redshift access to S3
                 region = 'us-west-2',  # region where S3 bucket is located
                 json = 'auto',         
                 redshift_conn_id = '', # redshift connection
                 provide_context=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_iam_role = aws_iam_role
        self.region = region
        self.json = json
        self.redshift_conn_id = redshift_conn_id

        self.provide_context = provide_context

    def execute(self, context):
        execution_date = context.get('execution_date')
        s3_path = 's3://{s3_bucket}/{s3_key}/{year}/{month}/{year}-{month}-{day}-events.json'
        
        if self.s3_key == 'log-data':
            full_s3_path = s3_path.format(
                s3_bucket = self.s3_bucket,
                s3_key = self.s3_key,
                year = execution_date.strftime('%Y'),
                month = execution_date.strftime('%m'),
                day = execution_date.strftime('%d')
            )
        else:
            full_s3_path = 's3://{}/{}/A/A/A/'.format(
                self.s3_bucket,
                self.s3_key
            )

        formatted_sql = self.copy_sql.format(
            self.table,
            full_s3_path,
            self.aws_iam_role,
            self.region,
            self.json
        )
        
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Copying data from S3 to {self.table} table in Redshift Serverless')
        redshift_hook.run(formatted_sql)
        self.log.info(f'Finished copying data to {self.table} table')






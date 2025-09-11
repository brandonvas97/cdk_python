from aws_cdk import (
    # Duration,
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    aws_iam as iam,
    aws_lakeformation as lf,
    custom_resources as cr,
    aws_lambda_python_alpha as lambda_python,
    RemovalPolicy,
    Duration
    # aws_sqs as sqs,
)
from constructs import Construct
from pathlib import Path

class CdkMpsGroupStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "CdkMpsGroupQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )

        #Creation of S3 Bucket for results of the external API
        bucket = s3.Bucket(
            self, "LambdaStorageBucket", #Logical ID of Event in CloudFormation
            bucket_name="lambda-storage-bucket-mps-group",
            removal_policy=RemovalPolicy.DESTROY, #Delete bucket and its content in case of destroy
            auto_delete_objects=True
        )

        #Creation of S3 bucket for Athena queries
        athena_results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name="athena-query-results-mps-group",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        #Creation of Lambda Function using a python file in lambda folder
        lambda_f = lambda_python.PythonFunction(
            self, "RequestLambdaFunction",
            entry=str(Path(__file__).parent.parent / "lambda"),
            runtime=_lambda.Runtime.PYTHON_3_11,
            index="lambda_function.py",
            handler="handler",
            timeout=Duration.seconds(30)
        )

        bucket.grant_write(lambda_f) #Give write permissions to the bucket

        #Assign Admin to the user that is proprietary of access and secret key used in cdk deployment
        lf.CfnDataLakeSettings(
            self, "LakeFormationSettings",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{self.account}:root"
                )
            ]
        )

        #Give permissions for AwsCustomResource to change the Lake Formation configuration
        policy = cr.AwsCustomResourcePolicy.from_statements([
            iam.PolicyStatement(
                actions=["lakeformation:PutDataLakeSettings"],
                resources=["*"]
            )
        ])

        # Use AwsCustomResource to remove Use only IAM access control for new databases and Use only IAM access control for new tables in new databases in Data Catalog settings
        cr.AwsCustomResource(
            self, "DisableIamOnlyAccessControl",
            on_create=cr.AwsSdkCall(
                service="LakeFormation",
                action="putDataLakeSettings",
                parameters={
                    "DataLakeSettings": {
                        "CreateDatabaseDefaultPermissions": [],
                        "CreateTableDefaultPermissions": []
                    }
                },
                physical_resource_id=cr.PhysicalResourceId.of("LakeFormationSettingsUpdated")
            ),
            policy=policy
        )


        #Creation of Glue database
        glue_db = glue.CfnDatabase(
            self, "ResultsDatabase",
            catalog_id=self.account, #Specify account where is created the DB 
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="results_db"
            ) #Name of the DB
        )
        
        #Creation of table
        glue_table = glue.CfnTable(
            self, "ResultsTable",
            catalog_id=self.account,
            database_name=glue_db.database_input.name,
            table_input=glue.CfnTable.TableInputProperty(
                name="results",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "typeOfData": "file"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{bucket.bucket_name}/results/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={"serialization.format": "1"}
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="name", type="string"),
                        glue.CfnTable.ColumnProperty(name="username", type="string"),
                        glue.CfnTable.ColumnProperty(name="email", type="string"),
                        glue.CfnTable.ColumnProperty(
                            name="address",
                            type="struct<street:string,suite:string,city:string,zipcode:string,geo:struct<lat:string,lng:string>>"
                        ),
                        glue.CfnTable.ColumnProperty(name="phone", type="string"),
                        glue.CfnTable.ColumnProperty(name="website", type="string"),
                        glue.CfnTable.ColumnProperty(
                            name="company",
                            type="struct<name:string,catchPhrase:string,bs:string>"
                        ),
                    ]
                )
            )
        )

        #Remove permissions to IAMAllowedPrincipals
        lf.CfnPermissions(
            self, "RevokeIAMAllowedPrincipalsOnTable",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier="IAM_ALLOWED_PRINCIPALS"
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    name=glue_table.table_input.name,
                    database_name=glue_db.database_input.name,
                    catalog_id=self.account
                )
            ),
            permissions=[],
            permissions_with_grant_option=[]
        ).add_dependency(glue_table)

        #Glue IAM Role to give permissions to the Crawler
        glue_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"), #Allow the Glue service to assume this role
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]#Assign the standard policies for Glue Crawlers, Glue, S3, Cloudwatch, etc...
        )

        #Give permissions to Glue role to read the S3 Bucket
        bucket.grant_read(glue_role)

        #Give permissions for glue_role to Lake formation
        lf.CfnPermissions(
            self, "GlueRoleDatabaseAdmin",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    name=glue_db.database_input.name,
                    catalog_id=self.account
                )
            ),
            permissions=["ALL"]
        )

        #Creation of Glue Crawler
        crawler = glue.CfnCrawler(
            self, "ResultsCrawler",
            role=glue_role.role_arn, #Give permissions to Crawler
            database_name=glue_db.database_input.name, #Indicate the Glue Database
            name="results-crawler",
            targets=glue.CfnCrawler.TargetsProperty(#Indicate the target resources of the crawler 
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty( #List of routes to scan in the S3 bucket
                        path=f"s3://{bucket.bucket_name}/results/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(#Establish the schedule of the crawler
                #schedule_expression="cron(0 */20 * * ? *)" #Hours
                schedule_expression="cron(0/5 * * * ? *)" #Minutes
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(#Policy of changes
                update_behavior="UPDATE_IN_DATABASE", #If the schema of the data changes, the table is updated too
                delete_behavior="DEPRECATE_IN_DATABASE" #If columns or files disappear, the table will be obsoleted, but not erased 
            ),
            configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}' #If there are multiple files with the same structure, these are used in only one table
        )

        #Create the athena user
        athena_user = iam.User(self, "AthenaUser", user_name="athena-user")

        #Give permissions for the athena user to Athena Service
        athena_user.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        )

        bucket.grant_read(athena_user)

        #Give permissions for the athena user to the database
        lf.CfnPermissions(
            self, "AthenaUserDatabasePermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_user.user_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    name=glue_db.database_input.name,
                    catalog_id=self.account
                )
            ),
            permissions=["DESCRIBE"]
        )
        

        #Give permissions for athena user to specific columns of the table
        lf_column_permissions = lf.CfnPermissions(
            self, "AthenaUserColumnPermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_user.user_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
                    database_name=glue_db.database_input.name,
                    name=glue_table.table_input.name,  #Table name
                    column_names=["id", "name", "email"],  #Columns
                    catalog_id=self.account
                )
            ),
            permissions=["SELECT"],
            permissions_with_grant_option=[]
        )

        lf_column_permissions.add_dependency(glue_table) #We add dependency with the table
        lf_column_permissions.apply_removal_policy(RemovalPolicy.RETAIN) #The columns permissions are erased before, so it's not necessary to include it in the destroy process
        
        #Configuration of Athena workgroup
        athena_workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name="glue-query-workgroup", 
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/results/"
                ),
                enforce_work_group_configuration=True,  # Force all queries use this configuration
            ),
            recursive_delete_option=True #Delete the workgroup history
        )

        #Give permissions to athena user to write in the s3 bucket
        athena_results_bucket.grant_read_write(athena_user)
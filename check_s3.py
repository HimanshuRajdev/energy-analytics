import boto3, os
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'], aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
r = s3.list_objects_v2(Bucket=os.environ['S3_BUCKET'], Prefix='raw/')
files = r.get('Contents', [])
print(f"Total S3 files: {len(files)}")
for dataset in ['retail-prices', 'generation', 'rto-demand']:
    count = len([f for f in files if f'raw/{dataset}/' in f['Key']])
    print(f"  {dataset}: {count} files")

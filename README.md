## Prerequisites
- AWS Account
- Python

## Deployment Steps

### 1. Create S3 Bucket

### 2. Create Aurora MySQL Database
- Create Aurora database
- Note endpoint, username, password

### 3. Deploy Lambda Functions
```bash
cd src/lambda
pip install pymysql -t .
zip -r TransformCSV.zip TransformCSV.py pymysql/
zip -r LoadCSV.zip LoadCSV.py pymysql/
zip -r QueryDB.zip QueryDB.py pymysql/
```
### Create Lambda functions (repeat for each)

### 4. Set Environment Variables for LoadCSV and QueryDB
{DB_HOST=your-endpoint,DB_USER=admin,DB_PASSWORD=yourpass,DB_NAME=SALES}

### 5. Create API Gateway Endpoints
- Create REST API in API Gateway Console
- Create resources and methods for each Lambda
- Deploy to stage

### 6. Configure VPC
- Add Lambda to same VPC as Aurora
- Create S3 Gateway Endpoint
- Configure security groups

### 7. Update callservice.sh
Replace these with your values:
- API Gateway URLs 
- S3 bucket name 

### 8. Run Pipeline
```bash
./test/callservice.sh
```


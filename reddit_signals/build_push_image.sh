echo "ECR address is: $1"
echo "dockername is: $2"
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $1
dockername=$2
docker build -t $dockername .
docker tag $dockername:'latest' $1/$dockername:'latest'
docker push $1/$dockername:'latest'

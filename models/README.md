# To save model in S3 from HuggingFace Hub

### Install Git Large File Storage
```
brew install git-lfs
```

### Verify that the installation was successful:
```
git lfs install
```

### Download a model
```
git clone git@hf.co:{repository}
```

### Create a tar file
```
cd {repository}
tar zcvf model.tar.gz *
```

### Upload model.tar.gz to S3
aws s3 cp model.tar.gz <s3://{my-s3-path}>

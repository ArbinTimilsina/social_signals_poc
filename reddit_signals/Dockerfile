FROM python:3.8-buster

RUN pip install --upgrade pip

# To copy the requirements.txt to the /plugins in docker
# We put requirements.txt in the working directory of container
ADD . /src
# To copy the code
ADD ./src /src
WORKDIR /src
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "entry_reddit_signals.py" ]

# APIs consumers guide

In this step by step guide we will explain how we created the API consumers that will fetch data from the API and send it to Kafka. Also we will see how we can check that kafka is receiving the data correctly and is possible to consume from the topics. To do it we will use the [Superhero API](https://superheroapi.com/) as an example. 

## Prerequisites
- Docker
- Kubernetes
- Kafka
- Python 3.11
- Python libraries: requests, kafka-python
- An account in [Docker Hub](https://hub.docker.com/) to upload the docker image

## Step 1: Create the API consumer
The first step is to create the API consumer that will fetch data from the API and send it to Kafka. We will use Python for this task. The code can be found in the file [producer-kafka.py](/kubernetes/producers/producer-kafka.py).

In this code we first import the necessary libraries and set the configuration variables. Which are:
- `API_URL`: The URL of the API we want to fetch data from.
- `TOPIC`: The Kafka topic where we want to send the data.
- `BOOTSTRAP`: The Kafka bootstrap servers.
- `POLL_SECONDS`: The number of seconds to wait between each API call.

Then we create a Kafka producer using the `KafkaProducer` class from the `kafka-python` library. We set the `bootstrap_servers` parameter to the value of the `BOOTSTRAP` variable and the `value_serializer` parameter to a lambda function that converts the data to JSON format and encodes it to UTF-8.

Next, we define a function `fetch_data` that takes a URL as a parameter and fetches data from the API using the `requests` library. If the request is successful, it returns the JSON data. If there is an error, it prints the error message and returns `None`.

Finally, we enter an infinite loop where we fetch data from the API using the `fetch_data` function. If the data is not `None`, we send it to Kafka using the `send` method of the producer. We also print a message indicating that the data has been sent. If there is an error sending the data to Kafka, we print the error message. We then wait for the number of seconds specified in the `POLL_SECONDS` variable before fetching data again.

## Step 2: Create the Docker image
The next step is to create a Docker image for the API consumer. We will use the [Dockerfile](/kubernetes/producers/Dockerfile) located in the `kubernetes/producers` directory. This Dockerfile uses the official Python 3.11 image as the base image. It sets the working directory to `/app`, copies the `requirements.txt` file to the container, and installs the required Python libraries using `pip`. Then it copies the `producer-kafka.py` file to the container, sets the `PYTHONUNBUFFERED` environment variable to `1` to ensure that the output is not buffered, and finally sets the command to run the `producer-kafka.py` script.

To build the Docker image, navigate to the `kubernetes/producers` directory and run the following command:

```bash
docker build -t yourusername/kafka-producer:latest .
```
Replace `yourusername` with your Docker Hub username.
Once the image is built, you can push it to Docker Hub using the following command:

```bash
docker push yourusername/kafka-producer:latest
```
In the example appears as `arejula11/kafka-producer:latest`. But if you want to use it, you have to replace `arejula11` with your own Docker Hub username.

## Step 3: Deploy the API consumer to Kubernetes
The final step is to deploy the API consumer to Kubernetes. We will use the [kafka-producers.yaml](/kubernetes/producers/kafka-producers.yaml) file located in the `kubernetes/producers` directory. This file defines a Kubernetes deployment for the API consumer. It specifies the number of replicas, the container image to use, and the environment variables to set. You need to replace `arejula11` with your own Docker Hub username in the `image` field. Also, you can adjust the number of replicas if you want to run multiple instances of the API consumer. And you can set different scripts to retrieve data from other APIs in new pods.

To deploy the API consumer to Kubernetes, navigate to the `kubernetes/producers` directory and run the following command:

```bash
kubectl apply -f kafka-producers.yaml
```
This will create the deployment and start the API consumer pods. You can check the status of the pods and the logs using the following command:

```bash
kubectl get pods -l app=kafka-producer
kubectl logs -l app=kafka-producer -f
```
You should see something like this:
![kafka-producer-logs](./assets/kafka-producer-logs-sending.png)

This indicates that the API consumer is successfully fetching data from the API and sending it to Kafka.

## Step 4: Verify data in Kafka
To verify that the data is being sent to Kafka correctly, you can use the Kafka console consumer. First, you need to get access to the Kafka pod. The easiest way is to use the VSCode Kubernetes extension to open a terminal in the Kafka pod. Once you have access to the Kafka pod, you can run the following command to consume messages from the `superheroes` topic:

```bash
kafka-console-consumer.sh --bootstrap-server kafka-g5:9092 --topic superheroes --from-beginning
```
You should see the messages being printed in the console as they are sent by the API consumer. This indicates that the data is being sent to Kafka correctly and is available for consumption. Furthermore, every each ten seconds you should see a new message being printed, as the API consumer is fetching data from the API every ten seconds. The messages should look like this:
```json
{
    "id": 208,
    "name": "Darth Vader",
    "slug": "208-darth-vader", 
    "powerstats": {
        "intelligence": 69,
        "strength": 48,
        "speed": 33,
        "durability": 35,
        "power": 100,
        "combat": 100
        },
        "appearance": {
            "gender": "Male",
            "race": "Cyborg",
            "height": ["6'6", "198 cm"],
            "weight": ["300 lb", "135 kg"],
            "eyeColor": "Yellow",
            "hairColor": "No Hair"
        },
        "biography": {
            "fullName": "Anakin Skywalker",
            "alterEgos": "No alter egos found.",
            "aliases": ["Lord Vader"],
            "placeOfBirth": "Tatooine",
            "firstAppearance": "Star Wars: Episode IV - A New Hope (1977)",
            "publisher": "George Lucas",
            "alignment": "bad"
        },
        "work": {
            "occupation": "Sith Lord, Supreme Commander of the Imperial Fleet",
            "base": "Death Star"
        },
        "connections": {
            "groupAffiliation": "Sith, Galactic Empire",
            "relatives": "Luke Skywalker (Son), Princess Leia (Daughter)"
        },
        "images": {
            "xs": "https://cdn.jsdelivr.net/gh/akabab/superhero-api@0.3.0/api/images/xs/208-darth-vader.jpg",
            "sm": "https://cdn.jsdelivr.net/gh/akabab/superhero-api@0.3.0/api/images/sm/208-darth-vader.jpg",
            "md": "https://cdn.jsdelivr.net/gh/akabab/superhero-api@0.3.0/api/images/md/208-darth-vader.jpg",
            "lg": "https://cdn.jsdelivr.net/gh/akabab/superhero-api@0.3.0/api/images/lg/208-darth-vader.jpg"
        }
}
```
## Conclusion
In this guide we have seen how to create an API consumer that fetches data from an API and sends it to Kafka. We have also seen how to create a Docker image for the API consumer and deploy it to Kubernetes. Finally, we have verified that the data is being sent to Kafka correctly by consuming messages from the Kafka topic. You can now use this setup to fetch data from any API and send it to Kafka for further processing.
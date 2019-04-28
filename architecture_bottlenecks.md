# **Overview Service Performance and Bottlenecks**

## Architecture

##### **Final Architecture**

![](https://i.imgur.com/4xjevgn.png "10k rps set up")

The server load is handled under a single t2.large load balancer configured with nginx. This fans out to one of six t2.micro instances hosting the service. The instances are built with Go and have a response latency < 10 ms without load. There are three routes on the server. `GET "/"` responds with a static HTML file that is powered by a React bundle hosted on AWS S3. `GET "/api/restaurants/overview/:id"`

## Stress Testing

The final architecture configuration handled a load 10k requests per second with 1231ms average latency and an error rate of 1.2%. Each request queried the api for one of 2.5 million restaurants.
![](https://i.imgur.com/SQADQL0.png "Proof")

Sub 1% error rates can be achieved with a request load of 9950 per second.

![](https://i.imgur.com/0XY180Z.png)

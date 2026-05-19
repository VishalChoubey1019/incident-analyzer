# Incident-Analyzer


Imagine you are oncall of your company. Something breaks at 2am. You start receiving calls(utter frustration). You open your laptop and see thousands of error logs. Where do you even start? What broke? Why? What do you fix first? That's the problem. This project solves it.

**How do errors even reach us?** </br>
First question you should ask: how does the system know something broke?
Services write logs. A log is just a line of text like ERROR: database connection failed. We need a way to collect those logs into one place.
So we build a simple HTTP endpoint -> the ingest service. Any service can POST its logs to us. That's all ingest/main.py does. It's literally just a receiver. Like a post box.

**Why not just process the log right there in the ingest service?** </br>
You could. But what if the processing is slow (like calling an AI)? The service sending the log is now waiting. What if 10000 logs arrive at once? You'd be overwhelmed.
So instead, the ingest service just drops the log into Kafka and immediately returns "accepted." It doesn't wait for anything.

**We have raw events in Kafka. Now what?** </br>
Here's the key insight: one error doesn't mean the service is broken. But multiple errors in short span (10 errors in 20 seconds) from the same service? That's an incident.
So we need something that watches the stream of events and says "hey, these 10 errors all came from order-service in the last 30 seconds — that's one incident."
That's correlation. That's what the correlator does.
The logic is simple:
- Keep a bucket per service
- Every error goes into that service's bucket
- Every 15 seconds, check each bucket
- If a bucket has 3+ errors, create an incident and empty the bucket
We are using flink for processing these although I ran a python script locally to avoid the build-and-redeploy cycle during development, and as much as I have tested it's work, I still created the script so that development is easy, with the flink I have to restart everything over and over again which is lethargic.

**We have an incident. Now we need the AI part.** </br>
We have an incident object that says "order-service had 10 errors, here are the error messages." But that's still raw data. An engineer waking up at 2am doesn't want raw data, they want someone to tell them what's wrong.
So we take that incident and send it to a language model with a prompt like: "you're a senior engineer, here's what happened, tell me the root cause and what to fix."
The model returns structured JSON with a summary, likely cause, and recommendations. We attach that to the incident.
Why Llama 3? Because it's free, runs offline, and is good enough at structured output.

**Store it** </br>
We now have an incident object, the original errors, the related metadata, and the AI analysis. We store it in our MongoDB.
**Why MongoDB?** Because the incident object is a nested JSON document, it has an RCA object and inside it there are a list of related event IDs, etc. MongoDB stores JSON natively. A relational DB would need multiple tables and joins for the same thing.

**Serve it** </br>
We need an API so the dashboard can fetch incidents. We built this as a gRPC API with proto3.
Why gRPC? Two reasons. One, it's what large companies use for internal APIs, second is because I wanted to understand it better. Also, protoBufs forces you to define your API contract explicitly, you can't just return random JSON, you have to define every field upfront which again is a hassle in the beginning but I have experienced it so I like it.

**Browsers can't speak gRPC** </br>
So we add an HTTP bridge in front of the gRPC server. The bridge receives normal HTTP requests from the dashboard and translates them into gRPC calls internally.
Think of it as a translator sitting between two people who speak different languages.

**Show it** </br>
This is all claude, I didn't want to code the dashboard as it is just HTML and JavaScript. Every 15 seconds it asks the bridge "give me all incidents." The bridge asks the gRPC server. The gRPC server reads from MongoDB. The result comes back as JSON and the dashboard renders it.

**To summarize the flow:** </br>
Error happens -> service sends it to ingest -> ingest drops it in Kafka -> correlator watches Kafka, groups errors by service -> when enough errors pile up it creates an incident -> AI engine picks up the incident, calls Llama 3, gets back a plain-English analysis -> saves to MongoDB -> gRPC server reads from MongoDB -> HTTP bridge translates to JSON -> dashboard shows it to you.

<img width="1727" height="903" alt="Screenshot 2026-05-19 at 12 29 02 AM" src="https://github.com/user-attachments/assets/5203d77d-b73f-4756-a4f2-7a47e6b71cd2" />

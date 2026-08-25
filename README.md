# Online Betting Real-Time Data Processing

## Project Overview

This project is a real-time data processing pipeline built using Apache Flink (PyFlink) and Amazon Kinesis Data Streams.

The main idea of this project is to process betting events in real time. A player can place multiple bets for a game, and later a game result is received. The pipeline keeps the betting events for each game and joins them with the game result when it arrives.

After joining the events, the pipeline calculates the payout and creates a new enriched event. The final processed event is sent to another Kinesis stream.

I built this project to understand how stateful stream processing works with Apache Flink and AWS Kinesis.

## Architecture

![Architecture](./architecture.png)

## What I Built

The pipeline has two input streams:

* `project_2_bet` - contains `PlayerBet` events
* `project_2_result` - contains `GameResult` events

The PyFlink application reads data from both streams.

The two streams are combined and keyed using `game_id`. This is important because all bets and the result belonging to the same game need to be processed together.

The application stores `PlayerBet` events in Flink state until the corresponding `GameResult` event arrives.

When the result arrives, the application gets the stored bets for that game, joins them with the result, calculates the payout and creates an enriched output event.

The processed data is then written to the `project_2_output` Kinesis stream.

## How the Pipeline Works

### 1. Generate Mock Data

I created a Python script called `mock_data_generator.py` to generate test betting data.

The script creates two types of events:

* `PlayerBet`
* `GameResult`

For a single game, the script generates multiple player bets and then sends a game result after a small delay.

It also creates some invalid events randomly. I added this because I wanted to test how the Flink application handles bad data.

The generator uses Boto3 to send the events to Amazon Kinesis. The streams used in the project are `project_2_bet` and `project_2_result`.

### 2. PlayerBet Events

A `PlayerBet` event contains information such as:

```json
{
    "event_type": "PlayerBet",
    "game_id": "game-123",
    "player_id": "player-5678",
    "bet_amount": 150.0,
    "geo_location": "IN",
    "platform": "mobile",
    "timestamp": "2026-01-01T12:30:00"
}
```

The generator can create multiple bets for the same `game_id`. This helps the Flink application test stateful processing.

### 3. GameResult Events

After the betting events are sent, the generator creates a `GameResult` event.

For example:

```json
{
    "event_type": "GameResult",
    "game_id": "game-123",
    "result": "Win",
    "multiplier": 2.0,
    "timestamp": "2026-01-01T12:30:05"
}
```

The result contains the final game result and a multiplier.

### 4. Read Data Using PyFlink

The main application is `app.py`.

It creates a Flink `StreamExecutionEnvironment` and uses the Kinesis connector to consume data from both input streams.
The application uses a JSON schema to convert the incoming Kinesis records into Flink rows.

The input fields include:

* `event_type`
* `game_id`
* `player_id`
* `bet_amount`
* `geo_location`
* `platform`
* `result`
* `multiplier`
* `timestamp`

### 5. Standardize Both Streams

The two streams have different information.

A `PlayerBet` has player and betting information, while a `GameResult` has result and multiplier information.

I converted both events into a common structure before processing them.

For `PlayerBet`, fields like `result` and `multiplier` are set to `None`.

For `GameResult`, fields like `player_id`, `bet_amount`, `geo_location` and `platform` are set to `None`.

This makes it easier to combine both streams later.

### 6. Combine and Key the Streams

After standardizing the two streams, I combined them using `union()`.

Then I used `game_id` as the key:

```python
combined_stream = (
    standardized_player_bets
    .union(standardized_game_results)
    .key_by(lambda x: x.game_id)
    .process(
        BettingProcessFunction(),
        output_type=output_type_info
    )
)
```

Using `game_id` means events belonging to the same game are processed together.

### 7. Store PlayerBet Events in Flink State

I used Flink `ListState` to temporarily store the betting events.

When a `PlayerBet` event arrives, it is added to the state for that game.

This allows the application to remember the bets until the corresponding game result arrives.

```python
self.bets_state.add(json.dumps(event))
```

This is one of the main parts of the project because it shows how stateful stream processing can be used in Flink.

### 8. Process GameResult

When a `GameResult` event arrives, the application gets the stored betting events.

It checks if there are any matching bets for that game.

If matching bets are found, each bet is processed with the game result.

The application also checks for invalid values.

For example, if the bet amount is less than or equal to zero, that record is skipped.

It also checks that the multiplier is greater than zero.

### 9. Calculate Payout

After a bet is matched with a game result, the payout is calculated using:

```text
payout = bet_amount × multiplier
```

For example:

```text
Bet Amount = 150
Multiplier = 2

Payout = 150 × 2
       = 300
```

The application also adds some extra information to the output event.

These fields include:

* `game_result`
* `payout`
* `event_time`
* `is_high_value_bet`

A bet is marked as a high-value bet when the amount is greater than `1000`.

### 10. Send Data to Output Stream

After processing, the enriched event is sent to the output Kinesis stream.

The output stream configured in the project is:

```text
project_2_output
```

The output schema contains:

```text
game_id
player_id
bet_amount
game_result
payout
geo_location
platform
event_time
is_high_value_bet
```

The Kinesis sink is configured in the PyFlink application to write these processed records to the output stream.

## Example Output

A processed event can look like this:

```json
{
    "game_id": "game-123",
    "player_id": "player-5678",
    "bet_amount": 150.0,
    "game_result": "Win",
    "payout": 300.0,
    "geo_location": "IN",
    "platform": "mobile",
    "event_time": "2026-01-01T12:30:05",
    "is_high_value_bet": "No"
}
```

This output contains information from both the `PlayerBet` and `GameResult` events.

## Invalid Data Handling

I also added invalid test data to check the processing logic.

The mock data generator can create:

* Negative bet amounts
* Missing player IDs
* Invalid game results
* Negative multipliers

For example, an invalid bet can contain a negative `bet_amount`, while an invalid result can contain a negative multiplier.

The Flink application checks these values before creating the final output.

This helped me understand how to handle bad records in a streaming pipeline.

## Project Files

### `app.py`

This is the main PyFlink application.

It contains:

* Kinesis source configuration
* JSON deserialization
* Stream processing
* Flink state management
* Event joining
* Payout calculation
* Output Kinesis sink

### `mock_data_generator.py`

This file creates sample betting and game result events and sends them to Kinesis.

It is mainly used for testing the real-time pipeline.

### `application_properties.json`

This file contains the Kinesis stream names and AWS region.

The project uses:

```text
project_2_bet
project_2_result
project_2_output
```

and the AWS region is:

```text
ap-south-1
```

The configuration also points the Flink runtime to `app.py` and the dependency JAR.

### `pom.xml`

I used Maven to manage the Java dependencies required by the PyFlink application.

The project includes the Flink Kinesis connector and uses Maven Shade Plugin to create a JAR containing the required dependencies.

## Project Structure

```text
betting-app-streaming/
│
├── app.py
├── mock_data_generator.py
├── application_properties.json
├── pom.xml
├── config.properties
├── assembly/
│   └── assembly.xml
├── requirements.txt
└── README.md
```

## Technologies Used

* Python
* Apache Flink
* PyFlink
* Amazon Kinesis Data Streams
* Boto3
* Maven
* Java/JAR dependencies
* AWS

## Main Concepts I Learned

Through this project, I learned how real-time streaming applications work.

The main concepts I worked with were:

* Real-time data ingestion
* Amazon Kinesis Data Streams
* PyFlink
* Stateful stream processing
* Flink `KeyedProcessFunction`
* Flink `ListState`
* Stream union
* Event matching using `game_id`
* Data validation
* Real-time transformation
* Payout calculation
* Kinesis sink

The main thing I learned from this project was how Flink can keep state while processing continuously arriving events. Instead of waiting for all data to arrive, the application can store an event and process it later when the related event comes.

## Build and Dependencies

The project uses Maven to build the required dependency JAR.

The `pom.xml` contains the Kinesis connector and Maven plugins used for packaging the project. The configuration creates a dependency JAR named `pyflink-dependencies`.

For local execution, the PyFlink application also loads the dependency JAR using:

```python
env.add_jars("file:///home/rudra/pyflink-dependencies.jar")
```

This is used when running the application in the local environment.

## Result

The final pipeline can continuously receive betting and game result events, keep related bets in Flink state, match them using `game_id`, calculate the payout and send the processed event to another Kinesis stream.

This project gave me practical experience with real-time data engineering using PyFlink and AWS Kinesis.

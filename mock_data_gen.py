import json
import random
import time
import boto3
from datetime import datetime, timedelta

# AWS Kinesis Stream Configurations
REGION_NAME = "ap-south-1"
PLAYER_BETS_STREAM = "project_2_bet"
GAME_RESULTS_STREAM = "project_2_result"

# Kinesis client
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

# Helper functions
def random_geo_location():
    """Generate random geo-location."""
    countries = ["US", "UK", "IN", "JP", "CA", "AU", "DE", "FR"]
    return random.choice(countries)

def random_platform():
    """Generate random platform."""
    platforms = ["mobile", "tablet", "desktop"]
    return random.choice(platforms)

def generate_player_bet(game_id):
    """Generate a PlayerBet event."""
    return {
        "event_type": "PlayerBet",
        "game_id": game_id,
        "player_id": f"player-{random.randint(1000, 9999)}",
        "bet_amount": round(random.uniform(5, 5000), 2),
        "geo_location": random_geo_location(),
        "platform": random_platform(),
        "timestamp": datetime.utcnow().isoformat(),
    }

#["event_type", "game_id", "player_id", "bet_amount", "geo_location", "platform", "result", "multiplier", "timestamp"]

def generate_game_result(game_id):
    """Generate a GameResult event."""
    return {
        "event_type": "GameResult",
        "game_id": game_id,
        "result": random.choice(["Win", "Loss", "Draw"]),
        "multiplier": round(random.uniform(1.0, 10.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }

def generate_invalid_player_bet():
    """Generate an invalid PlayerBet event."""
    return {
        "event_type": "PlayerBet",
        "game_id": f"game-{random.randint(1, 1000)}",
        "player_id": None,  # Invalid player_id
        "bet_amount": -random.uniform(1, 100),  # Invalid bet amount
        "geo_location": random_geo_location(),
        "platform": random_platform(),
        "timestamp": datetime.utcnow().isoformat(),
    }

def generate_invalid_game_result():
    """Generate an invalid GameResult event."""
    return {
        "event_type": "GameResult",
        "game_id": f"game-{random.randint(1, 1000)}",
        "result": None,  # Invalid result
        "multiplier": -random.uniform(1.0, 10.0),  # Invalid multiplier
        "timestamp": datetime.utcnow().isoformat(),
    }

def publish_event_to_kinesis(stream_name, event):
    """Publish event to Kinesis stream."""
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(event),
        PartitionKey=event["game_id"],
    )
    print(f"Published event to {stream_name}: {json.dumps(event)}")

def generate_mock_data():
    """Generate and publish mock data."""
    while True:
        game_id = f"game-{random.randint(1, 1000)}"

        # Generate multiple PlayerBet events for the same game_id
        for _ in range(random.randint(2, 5)):
            event = generate_player_bet(game_id)
            publish_event_to_kinesis(PLAYER_BETS_STREAM, event)

        # Occasionally generate invalid PlayerBet events
        if random.random() < 0.1:  # 10% chance
            invalid_event = generate_invalid_player_bet()
            publish_event_to_kinesis(PLAYER_BETS_STREAM, invalid_event)

        # Simulate some delay before sending GameResult
        time.sleep(random.uniform(1, 3))

        # Generate GameResult event for the same game_id
        result_event = generate_game_result(game_id)
        publish_event_to_kinesis(GAME_RESULTS_STREAM, result_event)

        # Occasionally generate invalid GameResult events
        if random.random() < 0.1:  # 10% chance
            invalid_result_event = generate_invalid_game_result()
            publish_event_to_kinesis(GAME_RESULTS_STREAM, invalid_result_event)

        # Introduce delay to simulate real-world streaming
        time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    generate_mock_data()
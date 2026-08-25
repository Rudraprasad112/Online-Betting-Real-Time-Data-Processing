import json
import os
import logging
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction,RuntimeContext
from pyflink.datastream.formats.json import JsonRowDeserializationSchema,JsonRowSerializationSchema
from pyflink.common import Types,Row
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer, KinesisStreamsSink,PartitionKeyGenerator
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Online-betting-processing")

env = StreamExecutionEnvironment.get_execution_environment()

Application_properties_file_path = "/etc/flink/application_properties.json"

is_local = True if os.environ.get("IS_LOCAL") else False

if is_local:
    print("start_local enviroment")
    Application_properties_file_path = "application_properties.json"
    # add dependcies for run flink code
    env.add_jars(f"file:///home/rudra/pyflink-dependencies.jar")


def get_application_properties():
    if os.path.isfile(Application_properties_file_path):
        with open(Application_properties_file_path) as file:
            print("application file exist")
            return json.load(file)
    else:
        return {}

def property_map(props,prop_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == prop_group_id:
            return prop['PropertyMap']

generalized_type_info = Types.ROW_NAMED(
    ["event_type", "game_id", "player_id", "bet_amount", "geo_location", "platform", "result", "multiplier", "timestamp"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.FLOAT(), Types.STRING()]
)

generalized_deserilization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(generalized_type_info) \
    .build()

# output_schema setup
output_type_info = Types.ROW_NAMED(
    ["game_id", "player_id", "bet_amount", "game_result", "payout", "geo_location", "platform", "event_time", "is_high_value_bet"],
    [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
)

output_serilization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(output_type_info) \
    .build()

class BettingProcessFunction(KeyedProcessFunction):
    def open(self,runtime_context:RuntimeContext):
        self.bets_state = runtime_context.get_list_state(
            ListStateDescriptor("bets_state",Types.STRING())
        )

    def process_element(self,value,ctx:KeyedProcessFunction.Context):
        """ process incomming events. """

        event = {
            "event_type": value.event_type,
            "game_id": value.game_id,
            "player_id": value.player_id,
            "bet_amount": value.bet_amount,
            "geo_location": value.geo_location,
            "platform": value.platform,
            "result": value.result,
            "multiplier": value.multiplier,
            "timestamp": value.timestamp,
        }

        event_type = event.get("event_type")
        game_id = event.get("game_id")

        if event_type == "PlayerBet":
            self.bets_state.add(json.dumps(event))
            logger.info(f"Stored PlayerBet event for game_id {game_id}: {event}")

        elif event_type == "GameResult":
            bets_events = list(self.bets_state.get())

            if bets_events:
                logger.info(f"Processing GameResult for game_id {game_id} with {len(bets_events)} PlayerBet events")

                for bet_event in bets_events:
                    bet_event = json.loads(bet_event)

                    if bet_event["bet_amount"] <= 0:
                        logger.info(f"invalid bet amount {bet_event['bet_amount']}")
                        continue

                    if "multiplier" not in event or event["multiplier"] <= 0:
                        logger.warning(f"Invalid multiplier in GameResult: {event}")
                        continue

                    joined_event = Row(
                        game_id=game_id,
                        player_id=bet_event["player_id"],
                        bet_amount=bet_event["bet_amount"],
                        game_result=event["result"],
                        payout=round(bet_event["bet_amount"] * event["multiplier"], 2),
                        geo_location=bet_event["geo_location"],
                        platform=bet_event["platform"],
                        event_time=datetime.utcnow().isoformat(),
                        is_high_value_bet="Yes" if bet_event["bet_amount"] > 1000 else "No",
                    )
                    logger.info(f"Join successful, emitting event: {joined_event}")
                    yield joined_event

                # clear the bet_state
                self.bets_state.clear()
            else:
                logger.info("no matching bets found in state")

def main():
    properites = get_application_properties()
    print(properites)
    bets_prop = property_map(properites,"Bet_stream_p2")
    game_result_prop = property_map(properites,"Result_stream_p2")
    output_prop = property_map(properites,"Output_stream_p2")

    print(bets_prop)
    print(game_result_prop)
    print(output_prop)
    
    bets_consumer = FlinkKinesisConsumer(
        bets_prop['stream.name'],
        generalized_deserilization_schema,
        {"aws.region":bets_prop['aws.region'],"stream.initpos":"LATEST"}
    )

    results_consumer = FlinkKinesisConsumer(
        game_result_prop['stream.name'],
        generalized_deserilization_schema,
        {"aws.region":game_result_prop['aws.region'],"stream.initpos":"LATEST"}
    )

    
    output_producer = KinesisStreamsSink.builder() \
        .set_stream_name(output_prop['stream.name']) \
        .set_serialization_schema(output_serilization_schema) \
        .set_kinesis_client_properties({"aws.region":output_prop['aws.region']}) \
        .set_partition_key_generator(PartitionKeyGenerator.random()) \
        .build()
    

    
    # defile flow
    players_bet = env.add_source(bets_consumer)
    game_result = env.add_source(results_consumer)

    standatdized_player_beats = players_bet.map(
        lambda event:Row(
            event_type=event.event_type,
            game_id=event.game_id,
            player_id=event.player_id,
            bet_amount=event.bet_amount,
            geo_location=event.geo_location,
            platform=event.platform,
            result=None,
            multiplier=None,
            timestamp=event.timestamp
        ),
        output_type=generalized_type_info
    )

    standatdized_game_results = game_result.map(
        lambda event:Row(
            event_type=event.event_type,
            game_id=event.game_id,
            player_id=None,
            bet_amount=None,
            geo_location=None,
            platform=None,
            result=event.result,
            multiplier=event.multiplier,
            timestamp=event.timestamp
        ),
        output_type=generalized_type_info

    )

    # unioun stream
    combained_stream = (
        standatdized_player_beats
        .union(standatdized_game_results)
        .key_by(lambda x:x.game_id)
        .process(BettingProcessFunction(),output_type=output_type_info)
    )

    combained_stream.sink_to(output_producer)


    logger.info("Starting Online Betting Stateful Processing")
    env.execute("Online Betting Stateful Processing")

if __name__ == "__main__":
    main()

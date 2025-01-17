import os
import json
import urllib.request
import boto3
from datetime import datetime, timezone


def format_player_profile(player):
    """
    Formats a player's profile data for display.

    Args:
        player (dict): Dictionary containing player information.

    Returns:
        str: Formatted player profile as a string.
    """
    name = player.get("Name", "Unknown")
    position = player.get("Position", "Unknown")
    team = player.get("Team", "Unknown")
    jersey = player.get("Number", "N/A")
    height = player.get("Height", "N/A")
    weight = player.get("Weight", "N/A")
    college = player.get("College", "N/A")
    experience = player.get("Experience", "N/A")
    status = player.get("Status", "N/A")

    return (
        f"Name: {name}\n"
        f"Position: {position}\n"
        f"Team: {team}\n"
        f"Jersey Number: {jersey}\n"
        f"Height: {height}\n"
        f"Weight: {weight}\n"
        f"College: {college}\n"
        f"Experience: {experience} years\n"
        f"Status: {status}\n"
    )


def fetch_nfl_players(api_key):
    """
    Fetches NFL player profiles from the API and formats them.

    Args:
        api_key (str): API key for the SportsData.io NFL API.

    Returns:
        str: Formatted profiles of all players.
    """
    api_url = f"https://api.sportsdata.io/v3/nfl/scores/json/PlayersByAvailable?key=882eab8bd9f047b5b11ffaeb9fd71eb9"

    try:
        # Fetch data from the API
        with urllib.request.urlopen(api_url) as response:
            players_data = json.loads(response.read().decode())

        if not players_data:
            return "No player data available."

        # Format each player's profile
        formatted_profiles = [format_player_profile(player) for player in players_data]
        return "\n---\n".join(formatted_profiles)

    except Exception as e:
        return f"Error fetching or processing data: {e}"


def split_message(message, chunk_size=250 * 1024):  # Split into chunks less than 256 KB
    return [message[i:i + chunk_size] for i in range(0, len(message), chunk_size)]

def lambda_handler(event, context):
    sns_client = boto3.client("sns")

    # Large message
    final_message = "..."  # Replace with your actual payload content

    # Split into smaller chunks
    chunks = split_message(final_message)

    for index, chunk in enumerate(chunks):
        try:
            sns_client.publish(
                TopicArn=os.getenv("SNS_TOPIC_ARN"),
                Message=chunk,
                Subject=f"Chunk {index + 1} of {len(chunks)} - NFL Player Data"
            )
        except Exception as e:
            print(f"Error publishing chunk {index + 1}: {e}")
            return {"statusCode": 500, "body": f"Error publishing chunk {index + 1}"}

    return {"statusCode": 200, "body": "All chunks sent to SNS successfully"}


if __name__ == "__main__":
    # API Key (Replace this with your actual API key)
    api_key = "882eab8bd9f047b5b11ffaeb9fd71eb9"

    # Fetch and display player profiles for debugging
    player_profiles = fetch_nfl_players(api_key)
    print(player_profiles)

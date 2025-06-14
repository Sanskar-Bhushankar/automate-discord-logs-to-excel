Step 1: Set Up a Discord Bot
Create a Discord Bot:
Go to the Discord Developer Portal.

Click New Application, give it a name (e.g., "RentalBot"), and create it.

Navigate to the Bot tab, click Add Bot, and confirm.

Under the Bot tab, enable the following Privileged Gateway Intents:
Presence Intent

Server Members Intent

Message Content Intent

Copy the Bot Token (you’ll need it for the code).

Invite the Bot to Your Server:
In the Developer Portal, go to the OAuth2 > URL Generator tab.

Select bot under Scopes.

Under Bot Permissions, select:
Read Messages/View Channels

Send Messages

Copy the generated URL, open it in a browser, and invite the bot to your server.

Step 2: Set Up Your Development Environment
Install Python:
Ensure Python 3.8+ is installed Download Python.

Verify installation by running python --version in your terminal.

Install Required Libraries:
Open a terminal and install the necessary Python libraries:
bash

pip install discord.py pandas openpyxl python-dotenv

discord.py: For interacting with Discord.

pandas: For handling Excel operations.

openpyxl: For writing to Excel files.

python-dotenv: For managing environment variables.

Create a Project Folder:
Create a folder (e.g., discord_rental_bot).

Inside it, create two files:
bot.py: The main bot script.

.env: To store your bot token securely.

Set Up the .env File:
Open .env and add your bot token:

DISCORD_TOKEN=your_bot_token_here

Replace your_bot_token_here with the token from the Discord Developer Portal.


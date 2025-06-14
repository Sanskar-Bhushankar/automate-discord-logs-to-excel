import discord
from discord.ext import commands, tasks
import pandas as pd
import os
from dotenv import load_dotenv
import asyncio
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
if not TOKEN:
    logging.error("DISCORD_TOKEN not found in .env file")
    exit(1)

# Set up bot with intents
intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Excel file path
EXCEL_FILE = 'rental_data.csv'

# Store previous Excel state to detect changes
previous_data = None

MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# Initialize Excel file if it doesn't exist
def init_excel():
    columns = {'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
    df_columns = list(columns.keys())
    if not os.path.exists(EXCEL_FILE):
        logging.info(f"Creating new CSV file: {EXCEL_FILE}")
        df = pd.DataFrame(columns=df_columns).astype(columns)
        for attempt in range(MAX_RETRIES):
            try:
                df.to_csv(EXCEL_FILE, index=False)
                logging.info(f"Successfully created {EXCEL_FILE}")
                break
            except PermissionError:
                logging.warning(f"Permission denied when creating {EXCEL_FILE}, retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logging.error(f"Error creating {EXCEL_FILE}: {str(e)}")
                exit(1)
        else:
            logging.error(f"Failed to create {EXCEL_FILE} after {MAX_RETRIES} attempts due to permission issues.")
            exit(1)
    else:
        # Verify existing file has correct columns and types
        for attempt in range(MAX_RETRIES):
            try:
                df = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=[''])
                # Re-apply dtypes for all columns, especially after reading CSV which might infer types differently
                df = df.astype(columns)
                missing_cols = [col for col in df_columns if col not in df.columns]
                if missing_cols:
                    logging.warning(f"Missing columns {missing_cols} in {EXCEL_FILE}. Adding them.")
                    for col in missing_cols:
                        df[col] = pd.NA if col == 'Message ID' else ''
                    df = df.astype(columns) # Re-apply astype after adding columns
                    df.to_csv(EXCEL_FILE, index=False)
                    logging.info(f"Successfully updated {EXCEL_FILE} with missing columns.")
                else:
                    # Ensure correct dtypes are applied even if columns exist
                    df = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=['']).astype(columns)
                    df.to_csv(EXCEL_FILE, index=False)
                    logging.info(f"Successfully ensured correct dtypes for {EXCEL_FILE}.")
                break # Exit retry loop on success
            except PermissionError:
                logging.warning(f"Permission denied when verifying/updating {EXCEL_FILE}, retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logging.error(f"Error reading or verifying {EXCEL_FILE}: {str(e)}")
                exit(1)
        else:
            logging.error(f"Failed to verify/update {EXCEL_FILE} after {MAX_RETRIES} attempts due to permission issues.")
            exit(1)

# Append data to Excel
def append_to_excel(data):
    df = None
    for attempt in range(MAX_RETRIES):
        try:
            # Read with specified dtypes to maintain consistency
            # For CSV, explicitly specify dtype for Message ID as pandas might infer differently
            df = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=[''])
            # Ensure all columns have their intended types after reading CSV
            columns = {'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
            df = df.astype(columns)
            break # Exit retry loop on successful read
        except FileNotFoundError:
            logging.warning(f"{EXCEL_FILE} not found, initializing... (Attempt {attempt + 1}/{MAX_RETRIES})")
            init_excel()
            # After init, try reading again in the next loop iteration
            continue 
        except PermissionError:
            logging.warning(f"Permission denied when reading {EXCEL_FILE}, retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Error reading {EXCEL_FILE}: {str(e)}")
            return
    else:
        logging.error(f"Failed to read {EXCEL_FILE} after {MAX_RETRIES} attempts due to permission issues.")
        return

    if df is None:
        logging.error("Failed to read CSV file, cannot append data.")
        return
    
    new_row = pd.DataFrame([data])
    # Ensure new_row has the same columns and dtypes as df
    columns_dict = { 'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
    for col in columns_dict.keys():
        if col not in new_row.columns:
            new_row[col] = pd.NA if col == 'Message ID' else ''
    new_row = new_row[df.columns].astype(dtype=columns_dict)

    df = pd.concat([df, new_row], ignore_index=True)
    for attempt in range(MAX_RETRIES):
        try:
            df.to_csv(EXCEL_FILE, index=False)
            logging.info("Appended new row to CSV successfully!")
            break
        except PermissionError:
            logging.warning(f"Permission denied when writing to {EXCEL_FILE}, retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Error writing to {EXCEL_FILE}: {str(e)}")
            return
    else:
        logging.error(f"Failed to write to {EXCEL_FILE} after {MAX_RETRIES} attempts due to permission issues.")

# Background task to monitor Excel for status changes
@tasks.loop(seconds=10)
async def check_excel_status():
    global previous_data
    try:
        # Read current CSV data with specified dtypes
        current_data = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=[''])
        columns = {'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
        current_data = current_data.astype(columns)
        logging.info("Checked CSV file for status changes")
        
        if previous_data is not None:
            # Compare rows to detect status changes
            for index, row in current_data.iterrows():
                message_id = str(row['Message ID']).strip() if pd.notna(row['Message ID']) else ""
                current_status = str(row['Status']).strip().lower()
                
                # Get previous status for this row
                # Ensure 'Message ID' is treated as string for comparison with previous_data
                previous_row = previous_data[previous_data['Message ID'].astype(str) == message_id]
                previous_status = str(previous_row['Status'].iloc[0]).strip().lower() if not previous_row.empty and pd.notna(previous_row['Status'].iloc[0]) else ""
                
                # Check if status has changed and is valid
                valid_statuses = ['issued', 'cancelled', 'delivered']
                if message_id and current_status != previous_status and current_status in valid_statuses:
                    logging.info(f"Status changed for Message ID {message_id}: {previous_status} -> {current_status}")
                    try:
                        # Fetch the original message
                        message = None
                        for guild in bot.guilds:
                            for channel in guild.text_channels:
                                try:
                                    message = await channel.fetch_message(int(message_id))
                                    break
                                except (discord.NotFound, ValueError):
                                    continue
                                except discord.Forbidden:
                                    logging.warning(f"No permission to access channel {channel.id}")
                                    continue
                        
                        if message:
                            await message.reply(f"Your order is {current_status.capitalize()}!")
                            logging.info(f"Sent status update for Message ID {message_id}")
                        else:
                            logging.warning(f"Message ID {message_id} not found or invalid")
                    except Exception as e:
                        logging.error(f"Error sending status update for Message ID {message_id}: {str(e)}")
        
        # Update previous_data, ensuring correct dtypes are maintained
        previous_data = current_data.copy()
    
    except FileNotFoundError:
        logging.error(f"{EXCEL_FILE} not found")
        init_excel()
    except PermissionError:
        logging.error(f"Permission denied when reading {EXCEL_FILE}")
    except Exception as e:
        logging.error(f"Error in check_excel_status: {str(e)}")

# Event: Bot is ready
@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    logging.info(f"Bot connected as {bot.user}")
    init_excel()
    global previous_data
    try:
        previous_data = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=[''])
        columns = {'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
        previous_data = previous_data.astype(columns)
    except FileNotFoundError:
        init_excel()
        previous_data = pd.read_csv(EXCEL_FILE, dtype={'Message ID': pd.Int64Dtype()}, na_values=[''])
        columns = {'Message ID': pd.Int64Dtype(), 'Name': str, 'Product Name': str, 'Rent or Buy': str, 'Phone No': str, 'Query': str, 'Status': str}
        previous_data = previous_data.astype(columns)
    except Exception as e:
        logging.error(f"Error initializing previous_data: {str(e)}")
        exit(1)
    if not check_excel_status.is_running():
        check_excel_status.start()
        logging.info("Started check_excel_status task")

# Event: Process messages
@bot.event
async def on_message(message):
    # Ignore messages from the bot itself
    if message.author == bot.user:
        return

    # Check if message starts with #rent or #buy
    if message.content.lower().startswith(('#rent', '#buy')):
        try:
            # Determine if it's a rent or buy request and remove the prefix
            is_rent = message.content.lower().startswith('#rent')
            content = message.content[len('#rent'):].strip() if is_rent else message.content[len('#buy'):].strip()
            
            # Split the message by commas
            parts = [part.strip() for part in content.split(',')]
            
            # Ensure exactly 5 parts (name, product name, rent or buy, phone no, query)
            if len(parts) != 5:
                await message.channel.send("Invalid format! Please use: #rent name,product name,rent or buy,phone no,query")
                return
            
            name, product_name, rent_or_buy, phone_no, query = parts
            
            # Validate rent_or_buy
            if rent_or_buy.lower() not in ['rent', 'buy']:
                await message.channel.send("Please specify 'rent' or 'buy' in the third field.")
                return
            
            # Store in Excel with Message ID and empty Status
            data = {
                'Message ID': message.id,
                'Name': name,
                'Product Name': product_name,
                'Rent or Buy': rent_or_buy.capitalize(),
                'Phone No': phone_no,
                'Query': query,
                'Status': ''  # Initialize status as an empty string
            }
            append_to_excel(data)
            
            await message.channel.send("Message recorded successfully!")
            logging.info(f"Processed message from {message.author}: {message.content}")
        
        except Exception as e:
            await message.channel.send(f"Error processing message: {str(e)}")
            logging.error(f"Error processing message: {str(e)}")
    
    # Process commands if any
    await bot.process_commands(message)

# Run the bot
bot.run(TOKEN)
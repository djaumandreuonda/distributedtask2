import rpyc, sqlite3
from rpyc.utils.server import ThreadedServer
from datetime import datetime

class BisaProcessor(rpyc.Service):
  card_database = {
    "8002 1235 5687 9898": ["08/23", "994", 98.99],
    "8002 6543 3456 7634": ["02/23", "456", 101.00],
    "8002 8945 2356 8345": ["02/25", "546", 56.00],
    "8002 6354 2345 8765": ["06/26", "134", 23.00]
  }

  def __init__(self):
    self.initialize_database()

  def is_expired(self, expiry):
        expiry_month, expiry_year = map(int, expiry.split("/"))

        current_year, current_month = datetime.now().year, datetime.now().month
        current_year = current_year % 100

        return current_year > expiry_year or (current_year == expiry_year and current_month > expiry_month)

  def matching_CVV(self, card_number, cvv):
        conn = sqlite3.connect('card_details.db')
        cursor = conn.cursor()

        cursor.execute("SELECT cvv FROM card_details WHERE card_num = ?", (card_number,))
        fetched_cvv = cursor.fetchone()

        conn.close()

        return fetched_cvv and fetched_cvv[0] == cvv

  def matching_card_number(self, card_number):
        conn = sqlite3.connect('card_details.db')
        cursor = conn.cursor()

        cursor.execute("SELECT card_num FROM card_details WHERE card_num = ?", (card_number,))
        fetched_card_number = cursor.fetchone()

        conn.close()

        return fetched_card_number is not None

  def is_balance_sufficient(self, card_number, amount):
        conn = sqlite3.connect('card_details.db')
        cursor = conn.cursor()

        cursor.execute("SELECT balance FROM card_details WHERE card_num = ?", (card_number,))
        fetched_balance = cursor.fetchone()
        conn.close()

        return fetched_balance and fetched_balance[0] >= amount

  def complete_transaction(self, card_num, amount):
    # Fetch the card details from the database
    conn = sqlite3.connect('card_details.db')
    cursor = conn.cursor()

    # Get the current balance of the card
    cursor.execute("SELECT balance FROM card_details WHERE card_num=?", (card_num,))
    current_balance = cursor.fetchone()[0]

    # Calculate the new balance
    new_balance = current_balance - amount

    # Update the balance in the database
    cursor.execute("UPDATE card_details SET balance=? WHERE card_num=?", (new_balance, card_num))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

  def exposed_bisa_processor(self, transaction_details):
    # Retrieve data from JSON
    card_num = transaction_details[
      "card_num"] if "card_num" in transaction_details else ""
    cvv = transaction_details["cvv"] if "cvv" in transaction_details else ""
    expiry = transaction_details[
      "expiry"] if "expiry" in transaction_details else ""
    amount = transaction_details[
      "amount"] if "amount" in transaction_details else 0

    # Checks for the transaction
    if not self.matching_card_number(card_num):
      return "Transaction Declined. Card number not found - NOT NEEDED METHOD"

    if not self.matching_CVV(card_num, cvv):
      return "Transaction Declined. Wrong CVV"

    if self.is_expired(expiry):
      return "Transaction Declined. Card expired"

    if not self.is_balance_sufficient(card_num, amount):
      return "Transaction Declined. Insufficient funds"

    # If all passes then complete transaction and return authorised to caller
    self.complete_transaction(card_num, amount)
    return "Transaction Authorised."

  def initialize_database(self):

    # Connect to the SQLite database file
    connection = sqlite3.connect('card_details.db')

    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Drop the card_details table if it exists
    cursor.execute("DROP TABLE IF EXISTS card_details")

    # Create the card_details table
    cursor.execute('''CREATE TABLE card_details
                       (card_num TEXT PRIMARY KEY, expiry_date TEXT, cvv TEXT, balance REAL)'''
                   )

    # Iterate through the card_database dictionary and insert the card details into the card_details table
    for card_num, details in self.card_database.items():
      expiry_date, cvv, balance = details
      cursor.execute(
        "INSERT INTO card_details (card_num, expiry_date, cvv, balance) VALUES (?, ?, ?, ?)",
        (card_num, expiry_date, cvv, balance))

    # Commit the changes and close the connection
    connection.commit()
    connection.close()

  def print_database(self):
    # Connect to the SQLite database file
    connection = sqlite3.connect('card_details.db')

    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Execute a SELECT query to retrieve all the rows from the card_details table
    cursor.execute("SELECT * FROM card_details")

    # Fetch all the rows as a list of tuples
    rows = cursor.fetchall()

    # Print the table column names
    print("Card Number\t Expiry Date\t CVV\t Balance")

    # Iterate through the rows and print the data
    for row in rows:
      card_num, expiry_date, cvv, balance = row
      print(f"{card_num}\t {expiry_date}\t {cvv}\t {balance}")

    # Close the connection
    connection.close()

if __name__ == '__main__':
  ts = ThreadedServer(BisaProcessor, port=18080)
  print('BisaProcessorService started on port 18080')
  ts.start()
  
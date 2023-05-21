import rpyc, json, uuid
from rpyc.utils.server import ThreadedServer


class PaymentProcessor(rpyc.Service):

  def exposed_process_payment(self, payment_info):
    card_num = payment_info["card_num"].replace(
      " ", "") if "card_num" in payment_info else ""
    if card_num.startswith("8002"):
      conn = rpyc.connect('44.199.11.132', 18080)
      response = conn.root.bisa_processor(payment_info)

      transaction_response = {
        "transaction_id": str(uuid.uuid4()),
        "response_type": "",
        "additional_info": response
      }

      if response.startswith("Transaction Declined."):
        transaction_response["response_type"] = "declined"
      if response.startswith("Transaction Authorised."):
        transaction_response["response_type"] = "accepted"

    else:
      transaction_response = {
        "transaction_id": str(uuid.uuid4()),
        "response_type": "declined",
        "additional_info": "Transaction not authorised. None Bisa card"
      }

    return json.dumps(transaction_response)


if __name__ == '__main__':
  ts = ThreadedServer(PaymentProcessor, port=18080)
  print('Service started on port 18080')
  ts.start()

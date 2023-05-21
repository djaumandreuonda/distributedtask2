import rpyc, json, uuid
from rpyc.utils.server import ThreadedServer

class PaymentProcessor(rpyc.Service):
  def exposed_process_payment(self, payment_info):
      card_num = payment_info["card_num"].replace(" ", "") if "card_num" in payment_info else ""
      if card_num.startswith("8002"):
        print("Bisa card")
        return "Bisa card"

      else:
        print("NOT KNOWN")
        return "NOT KNOWN"

if __name__ == '__main__':
    ts = ThreadedServer(PaymentProcessor,port=18080)
    print('Service started on port 18080')
    ts.start()
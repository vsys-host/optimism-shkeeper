
import decimal
import time
import copy
import requests
import eth_account 
from web3 import Web3, HTTPProvider
from decimal import Decimal

from celery.schedules import crontab
from celery.utils.log import get_task_logger
import requests as rq

from . import celery
from .config import config, get_min_token_transfer_threshold
from .models import Accounts, db
from .encryption import Encryption
from .token import Token, Coin, get_all_accounts
from .unlock_acc import get_account_password
from .utils import skip_if_running

logger = get_task_logger(__name__)

w3 = Web3(HTTPProvider(config["FULLNODE_URL"], 
                       request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

@celery.task()
def make_multipayout(symbol, payout_list, fee):
    if symbol == config["COIN_SYMBOL"]:
        coint_inst = Coin(symbol)
        payout_results = coint_inst.make_multipayout_eth(payout_list, fee)
        post_payout_results.delay(payout_results, symbol)
        return payout_results    
    elif symbol in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
        token_inst = Token(symbol)
        payout_results = token_inst.make_token_multipayout(payout_list, fee)
        post_payout_results.delay(payout_results, symbol)
        return payout_results    
    else:
        return [{"status": "error", 'msg': "Symbol is not in config"}]



@celery.task()
def post_payout_results(data, symbol):
    while True:
        try:
            return requests.post(
                f'http://{config["SHKEEPER_HOST"]}/api/v1/payoutnotify/{symbol}',
                headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']},
                json=data,
            )
        except Exception as e:
            logger.exception(f'Shkeeper payout notification failed: {e}')
            time.sleep(10)


@celery.task()
def refresh_balances():
    updated = 0

    try:
        from app import create_app
        app = create_app()
        app.app_context().push()

        list_acccounts = get_all_accounts()
        for account in list_acccounts:
            try:
                pd = Accounts.query.filter_by(address = account).first()
            except:
                db.session.rollback()
                raise Exception(f"There was exception during query to the database, try again later")

            acc_balance = decimal.Decimal(w3.from_wei(w3.eth.get_balance(account), "ether"))
            if Accounts.query.filter_by(address = account, crypto = config["COIN_SYMBOL"]).first():
                pd = Accounts.query.filter_by(address = account, crypto = config["COIN_SYMBOL"]).first()            
                pd.amount = decimal.Decimal(w3.from_wei(w3.eth.get_balance(account), "ether"))                     
                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()
            
            have_tokens = False
                
            for token in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
                token_instance = Token(token)
                if Accounts.query.filter_by(address = account, crypto = token).first():
                    pd = Accounts.query.filter_by(address = account, crypto = token).first()
                    balance = decimal.Decimal(token_instance.contract.functions.balanceOf(w3.to_checksum_address(account)).call())
                    normalized_balance = balance / decimal.Decimal(10** (token_instance.contract.functions.decimals().call()))
                    pd.amount = normalized_balance
                    
                    with app.app_context():
                        db.session.add(pd)
                        db.session.commit() 
                        db.session.close()  
                    if normalized_balance >= decimal.Decimal(get_min_token_transfer_threshold(token)):
                        have_tokens = copy.deepcopy(token)
                    
            if have_tokens in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
                drain_account.delay(have_tokens, account) 
            else:
                if acc_balance >= decimal.Decimal(config['MIN_TRANSFER_THRESHOLD']):
                    drain_account.delay(config["COIN_SYMBOL"], account)        
    
            updated = updated + 1                
    
            with app.app_context():
                db.session.add(pd)
                db.session.commit()
                db.session.close()
    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose()  
 
    return updated

@celery.task(bind=True)
@skip_if_running
def drain_account(self, symbol, account):
    logger.warning(f"Start draining from account {account} crypto {symbol}")
    # return False
    if symbol == config["COIN_SYMBOL"]:
        inst = Coin(symbol)
        destination = inst.get_fee_deposit_account()
        results = inst.drain_account(account, destination)
    elif symbol in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
        inst = Token(symbol)
        destination = inst.get_fee_deposit_account()
        results = inst.drain_tocken_account(account, destination)
    else:
        raise Exception(f"Symbol is not in config")
    
    return results


@celery.task(bind=True)
@skip_if_running
def create_fee_deposit_account(self):
    logger.warning(f"Creating fee-deposit account")
    inst = Coin(config["COIN_SYMBOL"])
    inst.set_fee_deposit_account()    
    return True
        


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # Update cached account balances
    sender.add_periodic_task(int(config['UPDATE_TOKEN_BALANCES_EVERY_SECONDS']), refresh_balances.s())



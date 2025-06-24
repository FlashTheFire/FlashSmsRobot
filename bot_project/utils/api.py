import os
from dotenv import load_dotenv

load_dotenv()


SMS_PROVIDERS = {
    "api1.5sim.net": os.getenv("5_SIM", None),
    "fastsms.su": os.getenv("FAST_SMS", None),
    "smshub.org": os.getenv("SMS_HUB", None),
    "api.grizzlysms.com": os.getenv("GRIZZLY_SMS", None),
    "smsbower.com": os.getenv("SMS_BOWER", None),
    "api.sms-activate.org": os.getenv("SMS_ACTIVATE", None),
    "vak-sms.com": os.getenv("VAK_SMS", None),
    "api.tiger-sms.com": os.getenv("TIGER_SMS", None)
}
SMS_PROVIDERS_ID = {
    "1": {"url": "api1.5sim.net", "api_key": os.getenv("5_SIM", None)},
    #"2": {"url": "fastsms.su", "api_key": os.getenv("FAST_SMS", None)},
    "3": {"url": "smshub.org", "api_key": os.getenv("SMS_HUB", None)},
    "4": {"url": "api.grizzlysms.com", "api_key": os.getenv("GRIZZLY_SMS", None)},
    "5": {"url": "smsbower.com", "api_key": os.getenv("SMS_BOWER", None)},
    "6": {"url": "api.sms-activate.org", "api_key": os.getenv("SMS_ACTIVATE", None)},
    #"7": {"url": "vak-sms.com", "api_key": os.getenv("VAK_SMS", None)},
    #"8": {"url": "api.tiger-sms.com", "api_key": os.getenv("TIGER_SMS", None)},
}
SMS_PROVIDERS_MANAGEMENT = {
    'FiveSimManagement', '1',
    #'FastSmsManagement', '2',
    'SmsHubManagement', '3',
    'GrizzlySmsManagement', '4',
    'SmsBowerManagement', '5',
    'SmsActivateManagement', '6',
    #'VakSmsManagement', '7',
    #'TigerSmsManagement', '8'
}
SMS_PROVIDERS_KEY = {
    'FiveSimManagement': '1',
    #'FastSmsManagement': '2',
    'SmsHubManagement': '3',
    'GrizzlySmsManagement': '4',
    'SmsBowerManagement': '5',
    'SmsActivateManagement': '6',
    #'VakSmsManagement': '7',
    #'TigerSmsManagement': '8'
}







FIVESIM = os.getenv("FIVESIM")
SMS_ACTIVATE = os.getenv("SMS_ACTIVATE")
FIVE_SIM = os.getenv("5_SIM")
FAST_SMS = os.getenv("FAST_SMS")
SMS_HUB = os.getenv("SMS_HUB")
GRIZZLY_SMS = os.getenv("GRIZZLY_SMS")
SMS_BOWER = os.getenv("SMS_BOWER")
VAK_SMS = os.getenv("VAK_SMS")
TIGER_SMS = os.getenv("TIGER_SMS")
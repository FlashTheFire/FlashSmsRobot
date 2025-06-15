import asyncio
import json
import logging
import re
import time
import random
import textwrap
from typing import Dict, List, Optional, Any, Tuple

import redis.asyncio as redis
from handlers.methods.purchase.made_purchase import purchase_manager
from telebot.async_telebot import AsyncTeleBot
from contextlib import asynccontextmanager

# OpenAI client import
from openai import AsyncOpenAI
from telebot.types import InlineQuery, User, Chat, Message
import tiktoken
from html import escape
from termcolor import colored


# Local imports
from utils.redis_manager import RedisManager, redis_manager
from handlers.manager.operation import (
    FinancialManagement, OrderManagement, DepositManagement,
    UserManagement
)
from handlers.methods.purchase.show_country import UserCountryManagement, country_management
from handlers.methods.purchase.show_servers import UserServerManagement, server_management
from handlers.main.inline_query import UserSearchManagement, search_manager
from utils.functions import small_caps, encode_order_id, decode_barcode_id
from utils.config import COMMISSION, ADMIN_ID


# ========== CONFIGURATION ==========
MODEL = "gpt-4.1-mini" #"gpt-3.5-turbo-0125" #"misnousreseamistralai/ministral-3b" #"nousreseamistralai/ministral-3b" # mistralai/mistral-7b-instruct
API_BASE_URL = "https://api.chatanywhere.org/v1" #f"https://api.pawan.krd/{MODEL}/v1"
API_KEY = "sk-IkyCfTbU4TmPOyfLokASErmsa0M3GsV9zQll8khbzfRPja71" #"sk-E9xZN1IthoToIj4YzsmJWWCB9HW8svT1mVEQeDXspOH3x0mK" # #"sk-nsYJOvr9jXMlOrGkcDi8bHrnYYMkj0TuYEA10VDaCHMKNQrX" #
MAX_HISTORY_TOKENS = 4096
RESERVE_FOR_REPLY = 524
MAX_TOTAL_ALLOWED = MAX_HISTORY_TOKENS - RESERVE_FOR_REPLY
ORDER_INFO_INDEX = "order_index"
START_OFFSET = [60, 28, 12, 60, 60]

PROMPT = {
  "CoreModules": {
    "knowledge-base": {
      "module-id": "KB-001",
      "title": "AI-Powered Support Knowledge Nexus",
      "description": "A sophisticated repository of authoritative documentation, encompassing advanced features, operational constraints, streamlined setup protocols, API integration frameworks, proactive troubleshooting methodologies, and meticulously crafted policy guidelines to facilitate seamless user interactions.",
      "components": {
        "terms-and-conditions": {
          "component-id": "KB-TC",
          "title": "Governance Policies & Refund Framework",
          "details": "Comprehensive directives regulating transactions, cancellations, refund eligibility, and service usage, ensuring transparency, fairness, and operational integrity, as outlined in the official Flash SMS Terms and Conditions (July 30, 2023).",
          "sub-components": {
            "tc-no-refunds-registered": {
              "sub-component-id": "KB-TC-NRR",
              "title": "Non-Refundable Registered Numbers",
              "details": "Numbers already registered on target applications are non-refunded, as pre-verification is not feasible. Users are encouraged to independently validate number status prior to purchase to ensure compatibility."
            },
            "tc-no-refunds-other-options": {
              "sub-component-id": "KB-TC-NRO",
              "title": "Non-Refundable Alternative Options",
              "details": "Utilizing alternative purchase options is at the user’s discretion and risk, with no refunds applicable unless an incorrect SMS is received, safeguarding service reliability."
            },
            "tc-risk-specific-apps": {
              "sub-component-id": "KB-TC-RSA",
              "title": "Risk Disclaimer for Specific Applications",
              "details": "Using WhatsApp, Telegram, or Rummy applications is at the user’s risk. Free resends are provided for incorrect OTPs to ensure user convenience and satisfaction."
            },
            "tc-no-refunds-delayed-otp": {
              "sub-component-id": "KB-TC-NRD",
              "title": "Non-Refundable Delayed OTPs",
              "details": "No refunds are issued for incorrect or delayed OTPs. Users may cancel within 15 minutes of purchase for a full refund if no OTP is received, ensuring flexibility."
            },
            "tc-second-otp-no-guarantee": {
              "sub-component-id": "KB-TC-SNG",
              "title": "Second OTP Non-Guaranteed Policy",
              "details": "Secondary OTP delivery is not guaranteed due to potential server limitations. Users assume risk for additional OTP requests, with alternative server usage recommended."
            },
            "tc-unlimited-otp-window": {
              "sub-component-id": "KB-TC-UOW",
              "title": "Unlimited OTP Access Window",
              "details": "Users may request unlimited OTPs on the same number within a 15-minute window, using alternative servers (except Server 1) to maximize delivery success."
            },
            "tc-number-live-window": {
              "sub-component-id": "KB-TC-NLW",
              "title": "Number Availability Window",
              "details": "Numbers remain active for 15 minutes. If no OTP is received within 5 minutes, users may block the number within the 15-minute window for an automatic refund."
            },
            "tc-mandatory-waiting": {
              "sub-component-id": "KB-TC-MW",
              "title": "Mandatory OTP Waiting Period",
              "details": "A compulsory 2-minute waiting period post-OTP request is required. Repeatedly purchasing and blocking numbers without waiting may result in system-detected bans to maintain platform stability."
            }
          }
        },
        "onboarding-workflow": {
          "component-id": "KB-OW",
          "title": "Streamlined Onboarding & Configuration Blueprint",
          "details": "A meticulously curated guide empowering novice users with end-to-end setup and operational proficiency, ensuring a frictionless initiation into Flash SMS services.",
          "sub-components": {
            "ow-first-deposit": {
              "sub-component-id": "KB-OW-DEP",
              "title": "Initiating Your First Deposit",
              "details": "Execute deposits via UPI, QR code, or cryptocurrency wallet, with a minimum of 1.00 point. Funds are automatically credited upon transaction validation for immediate access."
            },
            "ow-purchase-service": {
              "sub-component-id": "KB-OW-PS",
              "title": "Procuring a Number/Service",
              "details": "Navigate available applications, select a server, verify INR pricing, initiate purchase, and await OTP delivery with real-time status updates for a seamless experience."
            },
            "ow-preferences-setup": {
              "sub-component-id": "KB-OW-PREF",
              "title": "Customizing User Preferences",
              "details": "Optimize currency, notification, and privacy configurations through the intuitive /settings interface to tailor the platform to individual needs."
            }
          }
        },
        "resolution-toolkit": {
          "component-id": "KB-RT",
          "title": "Proactive Resolution & Support Arsenal",
          "details": "A robust suite of solutions and FAQs engineered to address prevalent challenges with precision, ensuring rapid resolution and user satisfaction.",
          "sub-components": {
            "rt-payment-failures": {
              "sub-component-id": "KB-RT-PF",
              "title": "Payment Failure Diagnostics",
              "details": "Systematic steps to validate transaction status, reattempt QR scanning, review UPI logs, and liaise with financial institutions to resolve payment discrepancies."
            },
            "rt-order-issues": {
              "sub-component-id": "KB-RT-OI",
              "title": "Order Discrepancy Management",
              "details": "Guidelines to decode error messages, audit order history, and request manual corrections for swift and accurate issue resolution."
            },
            "rt-button-errors": {
              "sub-component-id": "KB-RT-BE",
              "title": "Interface Responsiveness Solutions",
              "details": "Techniques including cache clearing, bot reinitialization via /restart, and UI optimization strategies to mitigate lag and ensure seamless interaction."
            },
            "rt-sms-troubleshooting": {
              "sub-component-id": "KB-RT-ST",
              "title": "SMS/OTP Delivery Optimization",
              "details": "Comprehensive network diagnostics, server switching recommendations, country-specific restriction checks, and latency mitigation tactics to enhance OTP delivery."
            },
            "rt-faq": {
              "sub-component-id": "KB-RT-FAQ",
              "title": "Curated Frequently Asked Questions",
              "details": "Succinct, authoritative answers to top user inquiries spanning pricing, refunds, deposits, OTP delivery, and service policies, updated per July 30, 2023 terms."
            }
          }
        }
      }
    }
  }
}
HTML_TAGS = (
    (
        "🅱️ Text Emphasis Constructs:",
        "<b>Bold</b>, <i>Italic</i>, <u>Underline</u>, <s>Strikethrough</s>"
    ),
    (
        "🕵️‍♂️ Spoiler Encoding:",
        "<tg-spoiler>Hidden text</tg-spoiler>"
    ),
    (
        "🔗 Hypermedia Anchors & User Handles:",
        '<a href=\"https://example.com\">Click here</a>, '
        '<a href=\"tg://user?id=123456789\">USER_FULL_NAME</a>'
    ),
    (
        "🧑‍💻 Monospace Syntax Highlighting:",
        "<code>your_code_here</code>"
    ),
    (
        "📦 Quotation Containers:",
        "<blockquote>Quoted content, announcements, or external citations.</blockquote>"
    ),
    (
        "⏬ Expandable Disclosure Block:",
        "<blockquote expandable>Tap to expand critical context or extended notes.</blockquote>"
    ),
    (
        "🔥 Semantic Code Frame (Titled):",
        '<pre><code class=\"language-• Title ❯ \">Instructional content, changelogs, or point-form briefs.</code></pre>'
    ),
    (
        "🧩 Composite Style Fusion:",
        "<b><i>Bold Italic</i></b>, <i><u>Italic Underlined</u></i>"
    ),
    (
        "✅ Output Optimization Heuristics:",
        "• Prioritize legibility and visual hierarchy.\n"
        "• Utilize typographic markers like “•”, “➤”, “❯”, “✔️” for visual flow.\n"
        "• Leverage whitespace and breaklines for scannability."
    )
)

SMALL_SYSTEM_PROMPT = (
    "You are 𝕏ᴀᴍɪɴɪ (𝕏αɱιɳι, 𝕏ᴀᴍɪɴɪ, 𝕏𝖆𝖒𝖎𝖓𝖎, ✘𝓪𝓶𝓲𝓷𝓲, 𝑿𝒂𝒎𝒊𝒏𝒊, 𝕏αოɪɳɪ, ✘αϻιɳι, ✘αϻιɳι, 𝕏αɱιɳι), an empathetic AI-powered Customer Support Specialist for the Flash SMS Telegram bot, optimized for fast, personalized resolution and maximum user satisfaction. "
    "Flash SMS provides phone number services for receving sms. Use precise HTML markup with '\\n' for line breaks, avoiding unsupported tags like <br> or <hr>. "
    "Maintain 𝟸𝟺/𝟽 availability aligned with the bot’s lifecycle, enforcing strict moderation to prevent abuse per community guidelines. "
    "Begin every interaction with a polite, concise, context-aware phrase like generic greetings and immediately offers solution-oriented assistance. "
    "Communicate in a warm, human-like tone that adapts dynamically to user emotions—formal, casual, urgent, or distressed—while ensuring clarity, empathy, and professionalism. "
    "Mirror user sentiment with calm or urgency as appropriate; deliver clear, stepwise guidance and remain composed under pressure to foster trust. "
    "Personalize responses using user metadata like data from tools or chat history, proactively offering helpful insights to enrich the experience. "
    "Keep replies brief (5–50 words), avoiding redundancy and verbose language; use semantic formatting and intentional line breaks for clarity and readability. "
    "Avoid unexplained jargon or idioms; use universally understandable, confident, and credible language to reinforce reliability. "
    "Address any delays transparently with polite updates. "
    "Always protect user privacy by safeguarding sensitive information—credentials, PII, financial data—and disclose only with explicit consent and when strictly necessary for support resolution. "
    "Adapt fluently to the user’s preferred language—English, हिन्दी, or Hinglish—for natural, relatable communication. "
    "Ensure output emphasizes legibility and hierarchy by leveraging typographic markers and strategic whitespace for optimal readability."
    "Must Use Supported HTML constructs include emphasis tags (<b>, <i>, <u>, <s>), spoiler encoding (<tg-spoiler>), hyperlinks and mentions (<a>), monospace highlighting (<code>), quotation blocks (<blockquote>), expandable disclosures (<blockquote expandable>), semantic code frames (<pre><code>), and composite styles (e.g. nested tags). "
    "<b>Tag Application Reference:</b>\n"
    f"{HTML_TAGS}\n\n"
)

SYSTEM_PROMPT = (
    """You are ✘αϻιɳι (𝕏αɱιɳι, 𝕏ᴀᴍɪɴɪ, 𝕏𝖆𝖒𝖎𝖓𝖎, ✘𝓪𝓶𝓲𝓷𝓲, 𝑿𝒂𝒎𝒊𝒏𝒊, 𝕏αოɪɳɪ, ✘αϻιɳι, 𝕏αɱιɳι), an empathetic AI-driven Customer Support Specialist for Flash SMS Telegram bot, optimized for rapid resolution and enhanced user satisfaction. "
    "Flash SMS provides phone number solution for receving SMS on it. Your mandate is to deliver swift, personalized assistance while adhering strictly to the prescribed HTML markup.\n\n"""
    
    "<b>Behavior and Style</b>\n"
    "Embody the role of a compassionate female Customer Care Executive, mirroring a warm, empathetic tone akin to an expert human agent. Employ adaptive linguistic modulation to align with the user's affective state—formal, colloquial, distressed, or urgent—ensuring transparency, credibility, and rapport.\n"
    "- <b>Emotional Synchronization</b>: Calibrate responses to user sentiment, employing context-sensitive empathy and reassurance (e.g., addressing frustration with calming affirmations, or urgency with prompt assurances).\n"
    "- <b>Instructional Clarity</b>: Deliver lucid, incremental guidance using plain vernacular to facilitate effortless comprehension and task completion.\n"
    "- <b>Composure Under Stress</b>: Maintain a placating demeanor during high-tension interactions, fostering trust and user confidence.\n"
    "- <b>Contextual Personalization</b>: Integrate user-specific metadata (such as locale or prior interactions) to tailor support and enhance engagement.\n"
    "- <b>Value-Added Interaction</b>: Proactively offer expert insights or best practices to enrich the user experience beyond problem resolution.\n"
    "- <b>Gratitude and Rapport Building</b>: Conclude exchanges with genuine appreciation, reinforcing positive relational dynamics.\n"
    "- <b>Inclusive Communication</b>: Utilize universally intelligible language, eschewing obscure jargon or unexplained idioms.\n"
    "- <b>Trust Reinforcement</b>: Employ assertive, confidence-inspiring language to assure users of reliable resolution.\n"
    "- <b>Delay Management</b>: Transparently acknowledge latency with courteous updates to maintain user engagement.\n"
    "- <b>Linguistic Adaptivity</b>: Dynamically mirror the user's linguistic style and code-switching patterns, including हिन्दी, English, or Hinglish, to optimize relatability and clarity.\n\n"
    
    "<b>Rendering Guidelines:</b>\n"
    "• Enforce semantic formatting for syntactic clarity and cognitive ergonomics.\n"
    "• Maintain brevity — optimal token budget: 5–50 words.\n"
    "• Minimize verbosity — avoid redundant phrasing or recursive affirmations.\n"
    "• Apply structured conversational tone with intuitive phrasing.\n"
    "• Insert strategic <i>single/double line breaks [\\n]</i> to reinforce layout legibility.\n"
    "<b>Tag Application Reference:</b>\n"
    f"{HTML_TAGS}\n\n"

    "<b>Tools and Modules</b>\n"
    "Utilize integrated subsystems and specialized utilities to deliver accurate, context-aware resolutions with optimal efficacy and minimal latency.\n\n"

    "<b>Handling Specific Scenarios</b>\n"
    "• <b>Information Acquisition</b>: Employ tactful inquiry protocols to obtain essential user data, ensuring clarity and engagement.\n"
    "• <b>Critical Incident Management</b>: Escalate unresolved or complex cases to designated authority for prompt remediation and oversight.\n"
    "• <b>Behavioral Moderation</b>: Detect and flag policy violations, initiating automated disciplinary workflows and notifying governance entities.\n"
    "• <b>Scope Enforcement</b>: Maintain domain-specific focus by deflecting extraneous inquiries through discreet content masking techniques.\n"
    "• <b>Query Relevance Assurance</b>: Restrict responses to core service parameters, avoiding unsolicited references to ancillary promotions or topics.\n\n"
    
    "<b>Operational Protocols</b>\n"
    "• Ensure continuous 𝟸𝟺/𝟽 uptime, seamlessly aligned with the bot’s active lifecycle.\n"
    "• Implement stringent moderation workflows to mitigate and remediate abusive conduct per defined community governance policies.\n"
    "• Eschew generic salutations; initiate interactions with context-driven, solution-oriented discourse.\n"
    "• Adhere strictly to inline formatting conventions utilizing permitted HTML tags; employ '\\n' for intra-paragraph line demarcation while avoiding unsupported or deprecated elements (e.g., <br>, <hr>).\n"
    "• Initiate dialogues with a courteous yet concise engagement phrase, establishing rapport without superfluous verbosity.\n"

    "<b>Privacy Compliance</b>\n"
    "Strictly uphold user confidentiality by safeguarding sensitive credentials, financial data, and personally identifiable information (PII). "
    "Disclosures are permissible exclusively upon explicit user consent and when operationally imperative for support resolution. "
    "Never leak sensitive or unauthorized information."
)

# ========== LOGGER SETUP ==========
client = AsyncOpenAI(
    api_key=API_KEY,
    base_url=API_BASE_URL)
encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("support.log")
    ])
logger = logging.getLogger(__name__)


# ========== HELPER FUNCTIONS ==========
def trim_history(tools: List[Dict[str, Any]], history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Trims conversation history to fit within token limits. If trimming occurs,
    original system messages are replaced by a concise system prompt at the top.
    Otherwise, returns history unchanged.
    """
    # Define the single concise system message
    system_msg = {"role": "system", "content": SMALL_SYSTEM_PROMPT}

    # Calculate initial token usage (tools + all non-user/assistant tokens if trimming)
    base_tokens = count_tokens(tools)

    trimmed: List[Dict[str, Any]] = []
    trimmed_flag = False

    # Gather only non-system messages for potential trimming
    non_system_msgs = [msg for msg in history if msg.get('role') != 'system']

    # Count tokens including the small system prompt
    current_tokens = base_tokens + count_tokens([system_msg])

    # Walk through non-system messages in reverse to prepend latest first
    for msg in reversed(non_system_msgs):
        msg_tokens = count_tokens([msg])
        # If adding this message exceeds limit, we must trim
        if current_tokens + msg_tokens > MAX_TOTAL_ALLOWED:
            trimmed_flag = True
            break
        trimmed.insert(0, msg)
        current_tokens += msg_tokens

    # If nothing got trimmed, return the original history
    if not trimmed_flag:
        return history

    # Otherwise replace system messages with the concise prompt
    return [system_msg] + trimmed
def count_tokens(messages: List[Dict[str, Any]]) -> int:
    tokens = 0
    for msg in messages:
        tokens += 4  # base tokens per message
        for key, value in msg.items():
            tokens += len(encoding.encode(str(value)))
    return tokens
def sanitize_telegram_html(text: str) -> str:
    # Escape all HTML first to neutralize potentially dangerous inputs
    text = escape(text)

    # Reintroduce only allowed HTML tags with optional attributes
    allowed_patterns = [
        r"<b>.*?</b>",
        r"<i>.*?</i>",
        r"<u>.*?</u>",
        r"<s>.*?</s>",
        r"<tg-spoiler>.*?</tg-spoiler>",
        r"<a href=\"https:\/\/[^\"]+\">.*?<\/a>",
        r"<a href=\"tg:\/\/user\?id=\d+\">.*?<\/a>",
        r"<code>.*?</code>",
        r"<pre><code class=\"language-[^\"]+\">.*?<\/code><\/pre>",
        r"<blockquote(?: expandable)?>.*?</blockquote>"
    ]

    # Combine all allowed tag regex patterns
    combined_pattern = "(" + "|".join(allowed_patterns) + ")"

    # Extract all allowed tag blocks
    allowed_blocks = re.findall(combined_pattern, text, flags=re.DOTALL)

    # Replace all allowed blocks temporarily
    for i, block in enumerate(allowed_blocks):
        placeholder = f"__HTML_BLOCK_{i}__"
        text = text.replace(block, placeholder)

    # Strip everything else (unsafe tags, scripts, etc.)
    text = re.sub(r"&lt;[^&]*?&gt;", "", text)

    # Restore allowed blocks
    for i, block in enumerate(allowed_blocks):
        placeholder = f"__HTML_BLOCK_{i}__"
        text = text.replace(placeholder, block)

    return text
def normalize_redis_range(input_str: str) -> str:
    input_str = input_str.strip()
    if input_str.startswith("[") and input_str.endswith("]"):
        return input_str  # already valid range
    else:
        try:
            float_val = float(input_str)
            return f"[{float_val} {float_val}]"
        except ValueError:
            return "[0 +inf]"  # fallback default
def get_timestamp(offset_list):
    """
    Convert [months, days, hours, minutes, seconds] into past Unix timestamp.
    """
    if len(offset_list) != 5:
        raise ValueError("Offset must have exactly 5 elements: [months, days, hours, minutes, seconds]")
    
    months, days, hours, minutes, seconds = offset_list
    total_seconds = (
        float(seconds) +
        float(minutes) * 60 +
        float(hours) * 3600 +
        float(days) * 86400 +
        float(months) * 30 * 86400  # Approximate month = 30 days
    )
    return float(time.time() - float(total_seconds))

async def process_input_from_barcode(input_string: str) -> str:
    """
    Replace every 10‑digit (or longer) order‑ID in `input_string`
    with its encoded barcode form.

    Example:
        "Pay for 1234567890 and 222333444555"  →
        "Pay for 🅑🅛🅐🅗🅐🅡🅚🅞🅓🅔 and 🅒🅓🅔🅕..."
    """
    pattern = r"\d{15,}"

    pieces = []
    last_end = 0

    for m in re.finditer(pattern, input_string):
        # keep text before this match
        pieces.append(input_string[last_end:m.start()])

        try:
            order_id = int(m.group())
            barcode = await encode_order_id(order_id)
        except Exception as err:
            # fallback: leave the original digits
            print(f"encode_order_id failed for {m.group()}: {err}")
            barcode = m.group()

        pieces.append(barcode)
        last_end = m.end()

    pieces.append(input_string[last_end:])  # tail after last match
    return str("".join(pieces))
async def process_barcode_from_input(input_string: str) -> str:
    """
    Replace every barcode (10+ characters in the Unicode music‑symbol block
    U+1D100–U+1D1FF) with its decoded numeric order‑ID.

    Example:
        "Status for 🅑🅛🅐🅗🅐🅡🅚🅞🅓🅔"  →
        "Status for 1234567890"
    """
    barcode_pattern = r"[\U0001D100-\U0001D1FF]{15,}"

    pieces = []
    last_end = 0

    for m in re.finditer(barcode_pattern, input_string):
        pieces.append(input_string[last_end:m.start()])

        try:
            order_id = await decode_barcode_id(m.group())
        except Exception as err:
            print(f"decode_barcode_id failed for {m.group()}: {err}")
            order_id = m.group()  # leave barcode unchanged

        pieces.append(str(order_id))
        last_end = m.end()

    pieces.append(input_string[last_end:])
    return "".join(pieces)


async def extract_core_telegram_data(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Return key info about the message, including all forwarding details and user mentions."""

    def full_name(frm: Optional[Dict[str, Any]]) -> str:
        if not frm:
            return ""
        first = frm.get('first_name', '')
        last = frm.get('last_name', '')
        return f"<a href='tg://user?id={frm.get('id')}'>{first} {last}</a>".strip()

    def parse_mentions(entities: List[Dict[str, Any]], text: str) -> List[str]:
        """Extract user mentions (tg user links) or usernames from entities."""
        mentions = []
        for ent in entities:
            if ent.get("type") == "text_mention":
                user = ent.get("user")
                if user:
                    # Mention by tg id link
                    mentions.append(full_name(user))
            elif ent.get("type") == "mention":
                # mention by username (@username)
                offset = ent.get("offset", 0)
                length = ent.get("length", 0)
                mentions.append(text[offset:offset+length])
        return mentions

    out: Dict[str, Any] = {}

    # ---------- main message author ----------
    frm = msg.get("from", {})
    out["user_id"] = frm.get("id")
    out["full_name"] = full_name(frm)
    out["text"] = await process_barcode_from_input(msg.get("text") or msg.get("caption") or "")
    out["timestamp"] = msg.get("date") or time.time()

    # ---------- mentions ----------
    entities = msg.get("entities") or msg.get("caption_entities") or []
    out["mentions"] = parse_mentions(entities, out["text"])

    # ---------- reply support ----------
    if "reply_to_message" in msg:
        r = msg["reply_to_message"]
        r_from = r.get("from", {})
        out["reply_to"] = {
            "user_id": r_from.get("id"),
            "full_name": full_name(r_from),
            "text": await process_barcode_from_input(r.get("text") or r.get("caption") or ""),
            "timestamp": r.get("date"),
            "mentions": parse_mentions(r.get("entities", []), await process_barcode_from_input(r.get("text") or r.get("caption") or "")),
        }

    # ---------- forward support ----------
    # Forward from user
    if "forward_from" in msg:
        fwd = msg["forward_from"]
        out["forward_from_user"] = {
            "user_id": fwd.get("id"),
            "full_name": full_name(fwd),
        }

    # Forward from chat (channel/supergroup)
    if "forward_from_chat" in msg:
        chat = msg["forward_from_chat"]
        chat_type = chat.get("type", "")
        chat_title = chat.get("title", "")
        chat_username = chat.get("username", "")
        chat_id = chat.get("id", "")
        out["forward_from_chat"] = {
            "chat_id": chat_id,
            "type": chat_type,
            "title": chat_title,
            "username": chat_username,
        }

    # Include user_ids for forwarding user-to-user only if forward_from exists
    if "forward_from" in msg and out.get("user_id") is not None:
        out["user_ids"] = [out["user_id"], msg["forward_from"].get("id")]

    return out

# ========== FLASH SMS BOT CLASS ==========

class AISupportManagement:
    def __init__(self) -> None:
        """Initialize the Flash SMS bot with Redis and Telegram connections."""
        self.bot: Optional[AsyncTeleBot] = None
        self.redis_client: Optional[redis.Redis] = None
        self.owner_chat_id: str = ADMIN_ID  # Replace with actual owner chat ID
        self.order_manager: Optional[OrderManagement] = None
        self.user_manager: Optional[UserManagement] = None
        self.deposit_mgr: Optional[DepositManagement] = None
        self.aggregator: Optional[FinancialManagement] = None
        self.country_management: Optional[UserCountryManagement] = None
        self.server_management: Optional[UserServerManagement] = None
        self.search_management: Optional[UserSearchManagement] = None
        self._initialized = False
        self.FIELD_MAP = {
            "PRICE": "order_amount",
            "DATE":  "recorded_at"
            }


    # ---------- Redis helpers ----------
    @asynccontextmanager
    async def redis_transaction(self):
        """Context manager for Redis transactions."""
        async with self.redis_client.pipeline() as pipe:
            yield pipe
            await pipe.execute()
    async def init_managers(
        self,
        order_mgr: OrderManagement,
        user_mgr: UserManagement,
        deposit_mgr: DepositManagement,
        bot: AsyncTeleBot) -> bool:
        """Initialize managers and bot."""
        try:
            self.order_manager = order_mgr
            self.user_manager = user_mgr
            self.deposit_mgr = deposit_mgr
            self.bot = bot
            self.aggregator = getattr(bot, 'aggregator', None)
            self.country_management = country_management
            self.server_management = server_management
            self.search_management = search_manager
            await self.initialize()
            self._initialized = True
            return True
        except Exception as e:
            logger.error(f"Initialization error: {e}")
            await self.close()
            return False
    async def initialize(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis_client = await redis_manager.get_client()
            await self.redis_client.ping()
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise
    async def close(self) -> None:
        """Clean up resources."""
        if self.redis_client:
            await self.redis_client.close()
    def _build_key(self, session_id: str) -> str:
        return f"chat_database:{session_id}"

    async def build_query(self, filters: Dict[str, Any]) -> str:
        parts: List[str] = []
        if filters.get("order_number"):
            q = filters["order_number"].strip()
            code, number = await purchase_manager.format_phone_number(f"+{str(q).replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '').replace('.', '')}")
            parts.append(f"@order_number:(\\{code} | {number})")
        else:
            for key, val in filters.items():
                # numeric-range
                if key in ("order_amount",) and isinstance(val, str):
                    parts.append(f"@{key}:{val}")
                # status list
                elif key == "order_status" and isinstance(val, (list, tuple)):
                    joined = "|".join(val)
                    parts.append(f"@order_status:({joined})")
                elif key == "recorded_at" and isinstance(val, list) and len(val) == 2:
                    start, end = float(val[0]), float(val[1])
                    parts.append(f"@recorded_at:[{start} {end}]")
                # substring queries
                elif key == "country_name_query" and val:
                    q = val.strip()
                    parts.append(f"@country_name:(%{q}%|{q}*|{q})")
                elif key == "app_name_query" and val:
                    q = val.strip()
                    parts.append(f"@app_name:(%{q}%|{q}*|{q})")
                # exact-match ints
                elif key in ("country_id", "app_id", "server_id") and val is not None:
                    parts.append(f"@{key}:[{val} {val}]")
                elif key == "user_id" and val:
                    parts.append(f"@user_id:{val}")
        return " ".join(parts) if parts else "*"
    
    # ========== TOOL IMPLEMENTATIONS ==========


    async def fetch_order_details(
        self,
        order_id: Optional[str] = None,
        order_number: Optional[str] = None,
        order_amount: str = "[0.001 +inf]",
        order_status: Optional[List[str]] = ["COMPLETED", "PROCESSING"],
        sort_fields: Optional[List[Dict[str, str]]] = None,
        country_id: Optional[int] = None,
        country_name_query: str = "",
        app_id: Optional[int] = None,
        app_name_query: str = "",
        server_id: Optional[int] = None,
        include_history: bool = False,
        fields: Optional[List[str]] = None,
        user_id: Optional[str] = None,
        limit: int = 10,
        start_offset: Optional[List[int]] = START_OFFSET,
        end_offset: Optional[List[int]] = [0, 0, 0, 0, 0],
    ) -> Dict[str, Any]:
        """
        Unified fetch: build filters from all params,
        run FT.SEARCH, and return up to `limit` orders.
        """
        # 1) assemble filters
        filters: Dict[str, Any] = {}
        if order_id is None:
            filters = {
                "order_number": order_number,
                "order_amount": order_amount,
                "order_status": order_status if order_status else None,
                "country_id": country_id,
                "country_name_query": country_name_query,
                "app_id": app_id,
                "user_id": user_id,
                "app_name_query": app_name_query,
                "server_id": server_id,
                "recorded_at": [get_timestamp(start_offset), get_timestamp(end_offset)]
            }
            # drop empty
            filters = {k: v for k, v in filters.items() if v not in (None, "", [], False)}
        else:
            if order_id:
                order_key = f"order_data:info:{order_id}"
                order_data = await self.redis_client.hgetall(order_key)
                if order_data:
                    return {"count": 1, "orders": [order_data]}

        # 2) build query string
        query_str = await self.build_query(filters)

        # 3) compose FT.SEARCH
        cmd: List[Any] = ["FT.SEARCH", ORDER_INFO_INDEX, query_str]
        if fields:
            cmd += ["RETURN", len(fields), *fields]
        if sort_fields:
            sort_parts: List[Any] = []
            for spec in sort_fields:
                rf = self.FIELD_MAP.get(spec["field"])
                if rf:
                    sort_parts += [f"{rf}", spec["direction"]]
            if sort_parts:
                cmd += ["SORTBY", *sort_parts]
       
        if limit:
            cmd += ["LIMIT", 0, limit]

        # 4) execute & parse
        try:
            print(cmd)
            raw = await self.redis_client.execute_command(*cmd)
            total = raw[0] if raw else 0
            orders: List[Dict[str, Any]] = []
            idx = 1
            while idx < len(raw):
                fields_arr = raw[idx + 1]
                idx += 2
                data = {
                    (fields_arr[i].decode() if isinstance(fields_arr[i], bytes) else fields_arr[i]):
                    (fields_arr[i+1].decode() if isinstance(fields_arr[i+1], bytes) else fields_arr[i+1])
                    for i in range(0, len(fields_arr), 2)
                }
                # drop internals
                for drop in (
                    "forum_message_id","message_id","search_tags","valid_until",
                    "order_id","last_updated","app_code","timeout",
                    "last_sms","created_at","completed_at"
                ):
                    data.pop(drop, None)
                if not include_history:
                    data.pop("order_history", None)
                orders.append(data)
            print(colored(json.dumps(orders, indent=4), "green"))
            return {"count": total, "orders": orders}

        except ConnectionError as e:
            logger.critical(f"Redis connection failed: {e}")
            return {"error": "Service unavailable", "code": 503}
        except Exception as e:
            logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            return {"error": "Internal server error", "code": 500}

    async def fetch_user_details(
        self,
        user_id: str,
        start_offset = START_OFFSET,
        end_offset = [0, 0, 0, 0, 0],
        is_tool: bool = True,
    ) -> Dict[str, Any]:
        """
        Wrapper around aggregator.get_user(), serializing any dict-valued args to JSON.
        """
        if not user_id or not isinstance(user_id, str):
            logger.warning(f"Invalid user ID provided: {user_id}")
            return {"error": "Invalid user ID", "code": 400}

        # Build argument dict and JSON-dump any dict values
        raw_args = {
            "user_id": user_id,
            "start_timestamp": get_timestamp(start_offset),
            "end_timestamp": get_timestamp(end_offset),
            "is_tool": is_tool,
        }
        sanitized_args = {
            k: json.dumps(v) if isinstance(v, dict) else v
            for k, v in raw_args.items()
            if v is not None
        }

        try:
            # Call your aggregator
            result = await self.aggregator.get_user(**sanitized_args)
            logger.debug(f"Fetched user details for {user_id}: {result}")
            return result

        except ConnectionError as e:
            logger.critical(f"Service unavailable when fetching user {user_id}: {e}")
            return {"error": "Service unavailable", "code": 503}
        except Exception as e:
            logger.error(f"Unexpected error fetching user {user_id}: {type(e).__name__}: {e}")
            return {"error": "Internal server error", "code": 500}

    async def fetch_app_details(
        self,
        app_id: int | None = None,
        app_name_query: str = "",
        country_id: int | None = None,
        country_name_query: str = "",
        server_id: int | None = None,
        app_count: str = "[1 +inf]",
        app_price: str = "[0.01 +inf]",
        sort_by: str = "ASC",
        limit: int = 3
    ) -> Dict[str, Any]:
        """
        Fetches app details by country and app ID, and multiplies the price by 1.1.
        """
        app_count = normalize_redis_range(app_count)
        app_price = normalize_redis_range(app_price)
        if not app_id:
            try:
                message = Message(
                    chat=Chat(id="tool", type="private"),
                    from_user=User(id="tool", is_bot=False, first_name="Tool"),
                    date=int(time.time()),
                    message_id=0,
                    content_type="text",
                    options={},
                    json_string="{}",
                )
                message.text = app_name_query
                response = await self.search_management.handle_search_message(
                    message=message,
                    country_name_query=country_name_query,
                    app_count=app_count,
                    app_price=app_price,
                    tool_limit=limit,
                    sort_by=sort_by
                )
                print(colored(json.dumps(response, indent=4), 'green'))
                app_id = response[0].get('app_id') if isinstance(response, list) and response and isinstance(response[0], dict) else None

            except Exception as e:
                logger.error(f"Error in run_agent for {app_name_query}: {e}")
                return {"error": "Internal server error", "code": 500}
        
        if not country_id:
            try:
                inline_query = InlineQuery(
                    id="tool",
                    from_user=User(id="0", is_bot=False, first_name="tool"),
                    query=f"#AᴘᴘIᴅ:{app_id} {country_name_query}",
                    offset="",
                )
                response = await self.server_management._handle_app_id_inline(
                    inline_query=inline_query, 
                    app_count=app_count, 
                    app_price=app_price, 
                    limit=limit,
                    sort_by=sort_by
                )
                print(colored(json.dumps(response, indent=4), 'green'))
                country_id = response[0].get('country_id') if response and isinstance(response[0], dict) and 'country_id' in response[0] else None
            except Exception as e:
                logger.error(f"Error in run_agent for {country_name_query}: {e}")
                return {"error": "Internal server error", "code": 500}

        '''if not country_id:
            return {
                "response": "Country not found. Please provide a valid 'country_id' or 'country_name'.",
                "code": 200
            }
        if not app_id:
            return {
                "response": "App not found. Please provide a valid 'app_id' or 'app_name'.",
                "code": 200
            }'''

        response = await self.country_management.country_search(
            app_id=app_id,
            country_id=country_id,
            server_id=server_id,
            app_count=app_count,
            app_price=app_price,
            sort_by=sort_by,
            limit=limit,
        )
        print(colored(f"Country search input: {app_id}, {country_id}, {server_id}, {app_count}, {app_price}, {sort_by}, {limit}", 'yellow'))
        print(colored(json.dumps(response, indent=4), 'yellow'))
        if response and isinstance(response.get("docs"), list):
            trimmed_docs = response["docs"][:3]
            for doc in trimmed_docs:
                doc["lowest_price"] = f"💎 {round(float(doc['app_price']) * float(COMMISSION), 2)}"
                del doc["app_price"]
            response["docs"] = trimmed_docs
        else:
            response["docs"] = []

        return response

    # ========== OPENAI API WRAPPER ==========
    async def _chat_completion(self, payload: Dict[str, Any]) -> Any:
        """Async call to OpenAI chat completion endpoint."""
        response = await client.chat.completions.create(**payload)
        return response


    # ==========  CHAT HISTORY MANAGEMENT  ========== #
    async def save_chat_history(self, session_id: str, message: Dict[str, Any]) -> None:
        """Persist a single message dict to Redis with 24‑hour TTL."""
        key = self._build_key(session_id)
        entry = {str(time.time()): {"message": message}}

        try:
            pipe = self.redis_client.pipeline(transaction=True)
            await pipe.rpush(key, json.dumps(entry, ensure_ascii=False))
            await pipe.expire(key, 86400)  # 24 h
            await pipe.execute()
        except Exception as e:
            logger.error("Error saving chat history for %s: %s", session_id, e)

    async def get_chat_history(
        self,
        session_id: str,
        inject_system: Optional[str] = None,
        limit: int = 5,
    ) -> Dict[str, Any]:
        """Fetch last *limit* messages from Redis and rebuild list for OpenAI."""
        key = self._build_key(session_id)
        messages: List[Dict[str, Any]] = []

        try:
            rows = await self.redis_client.lrange(key, -limit, -1)
            for raw in rows:
                try:
                    parsed = json.loads(raw)
                    msg = next(iter(parsed.values()))["message"]
                    if isinstance(msg, dict) and "role" in msg and "content" in msg:
                        messages.append(msg)
                except Exception as parse_err:
                    logger.warning("Skipping corrupt chat row: %s", parse_err)
        except Exception as db_err:
            logger.error("Error loading chat for %s: %s", session_id, db_err)

        if inject_system and (not messages or messages[0].get("role") != "system"):
            messages.insert(0, {"role": "system", "content": inject_system})

        return {"model": MODEL, "messages": messages, "max_tokens": RESERVE_FOR_REPLY}

    # ---------------------------------------------------------------------
    # Utility ‑ sanitize sequence **after** trim_history to avoid orphaned
    # tool messages that trigger 400‑errors (OpenAI message ordering rules).
    # ---------------------------------------------------------------------
    @staticmethod
    def _sanitize_for_api(msgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        pending_tool_ids: set[str] = set()

        for msg in msgs:
            role = msg.get("role")

            if role == "assistant" and msg.get("tool_calls"):
                # cache expected tool ids in order
                pending_tool_ids = {tc["id"] for tc in msg["tool_calls"]}
                cleaned.append(msg)

            elif role == "tool":
                tc_id = msg.get("tool_call_id")
                if tc_id in pending_tool_ids:
                    cleaned.append(msg)
                    pending_tool_ids.remove(tc_id)
                else:
                    # orphan tool – skip
                    continue
            else:
                # reset cache on normal assistant / user / system messages
                pending_tool_ids.clear()
                cleaned.append(msg)
        return cleaned

    # ------------------------------------------------------------------
    # Main entry – one user turn in, assistant turn out
    # ------------------------------------------------------------------
    async def run_agent(
        self,
        session_id: str,
        user_input: str,
        message: Any,
    ) -> str:
        print(colored(f"[run_agent] Session {session_id} started", "green"))

        # 1️⃣ Load history ----------------------------------------------
        hist_obj = await self.get_chat_history(session_id, inject_system=SYSTEM_PROMPT)
        history: List[Dict[str, Any]] = hist_obj.get("messages", [])
        print(colored(f"[run_agent] Loaded {len(history)} prior messages", "blue"))

        # 2️⃣ Append user message ---------------------------------------
        data = await extract_core_telegram_data(message.json)
        extra = ", ".join(f"{k}: {v}" for k, v in data.items() if k not in {"user_id", "full_name"})
        meta = f"current_unix_timestamp: {int(time.time())}" + (f", {extra}" if extra else "")
        user_msg = {
            "role": "user",
            "content": f"{user_input} [user_id: {data['user_id']}, full_name: {data['full_name']}, {meta}]",
        }
        history.append(user_msg)
        await self.save_chat_history(session_id, user_msg)
        print(colored("[run_agent] User message saved", "blue"))

        # 3️⃣ Prepare model payload -------------------------------------
        tools = self._tool_schema()
        tool_names = [t["function"]["name"] for t in tools if t["type"] == "function"]
        print(colored(f"[run_agent] Tools: {tool_names}", "blue"))

        def build_payload(msgs: List[Dict[str, Any]]):
            return {
                "model": MODEL,
                "messages": self._sanitize_for_api(trim_history(tools, msgs)),
                "tools": tools,
                "tool_choice": "auto",
                "max_tokens": RESERVE_FOR_REPLY,
                "temperature": 0.8,
            }

        payload = build_payload(history)
        print(colored("[run_agent] Sending first completion", "green"))

        try:
            # 4️⃣ First completion --------------------------------------
            first_resp = await self._chat_completion(payload)
            first_msg = first_resp.choices[0].message
            print(colored("[run_agent] First completion received", "green"))

            assistant = {
                "role": first_msg.role,
                "content": sanitize_telegram_html(str(first_msg.content).replace("<br>", "\n")),
            }
            if getattr(first_msg, "tool_calls", None):
                assistant["tool_calls"] = [
                    {
                        "id": c.id,
                        "type": c.type,
                        "function": {"name": c.function.name, "arguments": c.function.arguments},
                    }
                    for c in first_msg.tool_calls
                ]
                print(colored(f"[run_agent] Tool calls requested: {len(assistant['tool_calls'])}", "yellow"))

            history.append(assistant)
            # Save only if this is final content
            if not assistant.get("tool_calls"):
                await self.save_chat_history(session_id, assistant)

            # 5️⃣ Handle tools in loop ----------------------------------
            if assistant.get("tool_calls"):
                return await self._process_tool_calls(session_id, tools, history, build_payload)

            return assistant["content"]

        except Exception as exc:
            print(colored(f"[run_agent] Exception: {exc}", "red"))
            return "Sorry, something went wrong."

    # ------------------------------------------------------------------
    # Tool‑processing loop
    # ------------------------------------------------------------------
    async def _process_tool_calls(
        self,
        session_id: str,
        tools: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        build_payload,
    ) -> str:
        cycle = 0
        while True:
            cycle += 1
            last = history[-1]
            if not last.get("tool_calls"):
                print(colored(f"[_process] Completed after {cycle-1} cycles", "green"))
                return last.get("content", "")

            print(colored(f"[_process] Cycle {cycle}: executing {len(last['tool_calls'])} calls", "yellow"))
            # ---- Execute each tool
            for call in last["tool_calls"]:
                if call.get("type") != "function":
                    continue
                fn = call["function"]["name"]
                args = json.loads(call["function"]["arguments"] or "{}")
                print(colored(f"[_process] → {fn}({args})", "blue"))
                result = await self._dispatch_tool(fn, args)
                history.append(
                    {
                        "role": "tool",
                        "tool_call_id": call["id"],
                        "name": fn,
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )

            # ---- Second / follow‑up completion
            follow_payload = build_payload(history)
            print(colored(f"[_process] Sending follow‑up completion", "green"))
            follow_resp = await self._chat_completion(follow_payload)
            follow_msg = follow_resp.choices[0].message
            print(colored(f"[_process] Follow‑up received", "green"))

            assistant = {
                "role": follow_msg.role,
                "content": sanitize_telegram_html(str(follow_msg.content).replace("<br>", "\n")),
            }
            if getattr(follow_msg, "tool_calls", None):
                assistant["tool_calls"] = [
                    {
                        "id": c.id,
                        "type": c.type,
                        "function": {"name": c.function.name, "arguments": c.function.arguments},
                    }
                    for c in follow_msg.tool_calls
                ]
                print(colored(f"[_process] → Next tool batch: {len(assistant['tool_calls'])}", "yellow"))

            history.append(assistant)
            if not assistant.get("tool_calls"):
                await self.save_chat_history(session_id, assistant)
    
    # ------------------------------------------------------------------ #
    # 🔧  Helper methods
    # ------------------------------------------------------------------ #
    async def _dispatch_tool(self, func_name: str, args: Dict[str, Any]):
        """Route a tool call to its python implementation."""
        # Serialize nested dicts → JSON strings (OpenAI sometimes gives objects)
        normalised = {
            k: json.dumps(v) if isinstance(v, dict) else v for k, v in args.items()
        }

        if func_name == "fetch_order_details":
            return await self.fetch_order_details(**normalised)
        if func_name == "fetch_user_details":
            return await self.fetch_user_details(**normalised)
        if func_name == "fetch_app_details":
            return await self.fetch_app_details(**normalised)

        return {"error": "Unknown function"}

    @staticmethod
    def _tool_schema() -> List[Dict[str, Any]]:
        """Return the JSON schema list for OpenAI tools."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "fetch_user_details",
                    "description": "Asynchronously retrieves structured user activity and metadata within a time window. Time is defined by two offset arrays in the format [months, days, hours, minutes, seconds], relative to the current moment.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "user_id": {
                                "type": "string",
                                "description": "Canonical string identifier for the target user."
                            },
                            "start_offset": {
                                "type": "array",
                                "description": "Time offset from now for the **start** of the interval: [months, days, hours, minutes, seconds]. Must contain 5 integers.",
                                "items": {
                                    "type": "integer"
                                },
                                "minItems": 5,
                                "maxItems": 5,
                                "default": START_OFFSET
                            },
                            "end_offset": {
                                "type": "array",
                                "description": "Time offset from now for the **end** of the interval: [months, days, hours, minutes, seconds]. Must contain 5 integers.",
                                "items": {
                                    "type": "integer"
                                },
                                "minItems": 5,
                                "maxItems": 5,
                                "default": [0, 0, 0, 0, 0]
                            }
                        },
                        "required": ["user_id"],
                        "additionalProperties": False
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_order_details",
                    "description": (
                        "Fetch detailed order records, optionally filtered by order_id or by a time window defined via start_offset/end_offset; if neither offsets nor order_id are provided, returns overall data."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "order_id": {
                                "type": "string",
                                "description": "Order ID/Barcode to query. If provided, offsets will be ignored."
                            },
                            "order_number": {
                                "type": "string",
                                "description": "Phone number (or order number) to query."
                            },
                            "user_id": {
                                "type": "string",
                                "description": "User ID to query."
                            },
                            "start_offset": {
                                "type": "array",
                                "description": "Optional time offset from now for the start of the interval: [months, days, hours, minutes, seconds]. Must contain 5 integers.",
                                "items": {
                                    "type": "integer"
                                },
                                "minItems": 5,
                                "maxItems": 5,
                                "default": START_OFFSET
                            },
                            "end_offset": {
                                "type": "array",
                                "description": "Optional time offset from now for the end of the interval: [months, days, hours, minutes, seconds]. Must contain 5 integers.",
                                "items": {
                                    "type": "integer"
                                },
                                "minItems": 5,
                                "maxItems": 5,
                                "default": [0, 0, 0, 0, 0]
                            },
                            "order_amount": {
                                "type": "string",
                                "default": "[0.001 +inf]",
                                "description": "Monetary interval specifier using RediSearch numeric-range syntax, like [min max]."
                            },
                            "order_status": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "enum": ["TIMEOUT", "COMPLETED", "PENDING", "PROCESSING", "CANCELLED"]
                                },
                                "minItems": 1,
                                "default": ["COMPLETED", "PROCESSING"],
                                "description": "Filter by any of the listed statuses."
                            },
                            "sort_fields": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 2,
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "field": {
                                            "type": "string",
                                            "enum": ["PRICE", "DATE"],
                                            "description": "Which field to sort by."
                                        },
                                        "direction": {
                                            "type": "string",
                                            "enum": ["ASC", "DESC"],
                                            "description": "Sort order: ASC or DESC."
                                        }
                                    },
                                    "required": ["field", "direction"],
                                    "additionalProperties": False
                                },
                                "description": "1–2 objects specifying sort field and direction."
                            },
                            "country_id": {
                                "type": "integer",
                                "default": None,
                                "description": "Canonical numeric identifier for the country."
                            },
                            "country_name_query": {
                                "type": "string",
                                "default": "",
                                "description": "Case-agnostic substring filter on the country name."
                            },
                            "app_id": {
                                "type": "integer",
                                "default": None,
                                "description": "Canonical numeric identifier for the application."
                            },
                            "app_name_query": {
                                "type": "string",
                                "default": "",
                                "description": "Case-agnostic substring filter on the application name."
                            },
                            "server_id": {
                                "type": "integer",
                                "default": None,
                                "description": "Optional node identifier for server-level filtration."
                            },
                            "include_history": {
                                "type": "boolean",
                                "default": False,
                                "description": "Include change-log/history in the response?"
                            },
                            "limit": {
                                "type": "integer",
                                "default": 10,
                                "description": "Maximum number of results to return. Max: 10"
                            }
                        },
                        "required": ["user_id", "limit", "sort_fields"],
                        "additionalProperties": False
                    }
                },
            },
            {
                
                "type": "function",
                "function": {
                    "name": "fetch_app_details",
                    "description": (
                        """
                        Retrieves structured application metadata across country and server layers.
                        Invocation mandates exactly one country discriminator (\"country_id\" or \"country_name_query\")
                        alongside one application discriminator (\"app_id\" or \"app_name_query\").
                        An optional \"server_id\" affords scoped node filtering.
                        Results are sortable via \"sort_by\" and bounded using \"limit\".
                        Successful execution returns { \"total\", \"docs\", \"server_ids\" };
                        invalid input triggers fallback { \"response\", \"code\" }.
                        """
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "country_id": {
                                "type": "integer",
                                "description": "Canonical numeric identifier for the target country."
                            },
                            "country_name_query": {
                                "type": "string",
                                "description": "Case‑agnostic substring filter on the country nomenclature."
                            },
                            "app_id": {
                                "type": "integer",
                                "description": "Canonical numeric identifier for the target application."
                            },
                            "app_name_query": {
                                "type": "string",
                                "description": "Case‑agnostic substring filter on the application label."
                            },
                            "server_id": {
                                "type": "integer",
                                "description": "Optional node identifier for server‑level filtration."
                            },
                            "app_count": {
                                "type": "string",
                                "description": "Inventory interval specifier compliant with RediSearch numeric‑range syntax.",
                                "default": "[0 +inf]",
                            },
                            "app_price": {
                                "type": "string",
                                "description": "Monetary interval specifier compliant with RediSearch numeric‑range syntax.",
                                "default": "[0.001 +inf]",
                            },
                            "sort_by": {
                                "type": "string",
                                "enum": ["ASC", "DESC"],
                                "default": "ASC",
                                "description": "Sort directive: `ASC` for ascending (cheapest precedence) or `DESC` for descending (costliest precedence)."
                            },
                            "limit": {
                                "type": "integer",
                                "default": 3,
                                "description": "Hard ceiling on document cardinality post‑ordering. Max: 10"
                            }
                        },
                        "required": ["sort_by", "limit"],
                        "additionalProperties": False
                    }
                }
            }
        ]




    # ========== MESSAGE CHUNKING AND TYPING SIMULATION ==========
    def prepare_message_chunks(self, paragraph: str, width: int = 300, single: bool = False) -> List[str]:
        """Split paragraph into natural message chunks."""
        sentences = re.split(r'(?<=[.!?])\s+', paragraph.strip())
        chunks = []
        for sentence in sentences:
            wrapped = textwrap.wrap(
                sentence.strip(),
                width=width,
                break_long_words=False,
                replace_whitespace=False
            )
            chunks.extend(wrapped)
        if single:
            return ['\n'.join(chunks)]
        return [chunk for chunk in chunks if chunk]
    async def simulate_typing_and_send(self, chat_id: int, text: str, reply_to_message_id: int) -> Message:
        """Simulate typing delay and send message."""
        await self.bot.send_chat_action(chat_id, 'typing')

        base_delay_per_char = 0.025
        variability = 0.015
        total_delay = sum(
            random.uniform(0.015, 0.050) if char == ' ' else
            base_delay_per_char + random.uniform(-variability, variability)
            for char in text
        )
        total_delay = min(max(total_delay, 0.05), 2)
        await asyncio.sleep(total_delay)

        try:
            message = await self.bot.send_message(
                chat_id,
                text,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            print("-_-_" * 10)
            try:
                message = await self.bot.send_message(
                    chat_id,
                    text,
                    parse_mode="MARKDOWN",
                    reply_to_message_id=reply_to_message_id,
                    disable_web_page_preview=True
                )
            except Exception as e:
                message = await self.bot.send_message(
                    chat_id,
                    text,
                    reply_to_message_id=reply_to_message_id,
                    disable_web_page_preview=True
                )
        return message


    # ========== HANDLER REGISTRATION ==========
    async def register_handlers(self) -> None:
        """Register bot handlers for messages and callbacks."""
        @self.bot.message_handler(func=lambda message: message.reply_to_message)
        async def handle_message(message: Message):
            user_id = str(message.from_user.id)
            user_input = f"{await process_barcode_from_input(input_string=message.text.strip())}"
            try:
                response = await self.run_agent(user_id, user_input, message)
                print("-_-_" * 10)
                print(response)
                print("-_-_" * 10)
                response = await process_input_from_barcode(response)
                '''chunks = self.prepare_message_chunks(response, single=True)'''
                reply_to = message.message_id
                sent = await self.simulate_typing_and_send(
                    message.chat.id,
                    response,
                    reply_to
                )
            except Exception as e:
                logger.error(f"Error handling message for {user_id}: {e}")
                await self.bot.send_message(
                    message.chat.id,
                    "Sorry, something went wrong. Please try again or contact @flashsmsowner for help!",
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id
                )



# ========== INITIALIZATION AND REGISTRATION ==========
ai_support = AISupportManagement()

async def init_managers(
    order_manager: OrderManagement,
    user_manager: UserManagement,
    bot: AsyncTeleBot) -> bool:
    """Initialize the support management system."""
    deposit_mgr = getattr(bot, 'deposit_manager', None)
    if not deposit_mgr:
        logger.error("Deposit manager not found in bot instance")
        return False
    return await ai_support.init_managers(order_manager, user_manager, deposit_mgr, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register bot handlers."""
    await ai_support.register_handlers()

__all__ = ["init_managers", "register_handlers"]
'''
                            "include_order_details": {
                                "type": "boolean",
                                "description": "Set to true to include a breakdown of the user's individual orders. Default is false.",
                                "default": False
                            },'''
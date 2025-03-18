from flask import Flask, request, jsonify
import json
import logging
import asyncio
import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load mappings at startup
with open('future/app_name_small.json', "r") as f:
    app_mapping = json.load(f)
with open('future/country_name_small.json', "r") as f:
    country_mapping = json.load(f)

def get_app_code(app_name):
    """
    Returns the app code for the given app name.
    If the mapping is a list, the first element is returned.
    """
    code = app_mapping.get(app_name)
    if code is None:
        return app_name  # or handle missing key as needed
    if isinstance(code, list):
        return code[0]
    return code

def transform_json_structure(data, country_map):
    """
    Transforms an input JSON structure into a predefined format.
    
    For each country:
      - The country name is mapped to its code using country_map (defaults to "none" if absent).
      - Each service in the country is processed only if its data is a dict.
        • The service key is resolved using get_app_code.
        • Only servers with a valid dictionary structure are processed.
        • Among valid servers, a selection is made:
             - Compute the average cost.
             - Then select servers with cost below the average.
             - Among those, filter servers with count above the average count.
             - If candidates exist, choose the one with the highest count/cost ratio;
               otherwise, choose the server with the lowest cost.
      The output for each service is built as:
          { service_code: { str(cost): str(count) } }
      And the selected server name is recorded.
    
    Returns:
        tuple: (transformed_data, selected_servers)
    """
    transformed = {}
    selected_servers = {}
    unvalid_servers = []

    for country, services in data.items():
        if not isinstance(services, dict):
            logging.error(f"Skipping country '{country}': expected dict but got {type(services).__name__}")
            continue

        try:
            country_code = country_map.get(country.lower(), "none")
            if country_code not in transformed:
                transformed[country_code] = {}
            
            for service, servers in services.items():
                service_code = get_app_code(service)
                if not service_code:
                    logging.error(f"Invalid service: {service}")
                    continue

                if not isinstance(servers, dict):
                    logging.error(f"Skipping service '{service}' in country '{country}': expected dict but got {type(servers).__name__}")
                    continue

                valid_servers = [
                    (float(details.get("cost", 0)), int(details.get("count", 0)), server_name)
                    for server_name, details in servers.items()
                    if isinstance(details, dict) and int(details.get("count", 0)) > -1
                ]
                
                if not valid_servers:
                    logging.error(f"No valid servers found for service: {service}")
                    continue
                
                avg_cost = sum(cost for cost, _, _ in valid_servers) / len(valid_servers)
                low_cost_servers = [s for s in valid_servers if s[0] < avg_cost]
                
                if low_cost_servers:
                    avg_count = sum(count for _, count, _ in low_cost_servers) / len(low_cost_servers)
                    candidates = [s for s in low_cost_servers if s[1] > avg_count]
                else:
                    candidates = []
                
                best = max(candidates or valid_servers, key=lambda s: s[1] / s[0] if s[0] != 0 else float('inf'))
                cost, count, server_name = best
                transformed[country_code][service_code] = {f"{cost:.2f}": str(count)}
                selected_servers[service_code] = server_name
        except Exception as e:
            logging.error(f"Error processing country '{country}': {e}")
            continue

    logging.info("Unselected Servers:")
    logging.info(unvalid_servers)
    return transformed, selected_servers

async def fetch_with_retry(url, retries=1):
    """
    Fetch JSON from a URL with asynchronous retry logic.
    Implements timeout handling, rate-limit checks, and exponential backoff.
    """
    # Initial timeout in seconds
    timeout_duration = 20
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, retries + 1):
            timeout = aiohttp.ClientTimeout(total=timeout_duration)
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 500:
                        return 'NO_NUMBER'
                    if resp.status != 200:
                        retry_after = resp.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        logging.warning(
                            f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries})."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    resp.raise_for_status()
                    raw_response = await resp.text()
                    return json.loads(raw_response)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
                if attempt < retries:
                    backoff = 2 ** (attempt - 1)
                    logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                    await asyncio.sleep(backoff)
                    timeout_duration *= 1.5
                else:
                    logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
                    return None
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
                if attempt < retries:
                    backoff = 2 ** (attempt - 1)
                    logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                    await asyncio.sleep(backoff)
                    timeout_duration *= 1.5
                else:
                    logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
                    return None

# Initialize Flask app
app = Flask(__name__)

@app.route('/stubs/handler_api.php', methods=['GET'])
def api_handler():
    action = request.args.get('action')

    if action == 'getCountries':
        # Provide some sample countries
        countries = {
            "0": "russia", "1": "ukraine", "2": "kazakhstan", "4": "philippines",
            "6": "indonesia", "7": "malaysia", "8": "kenya", "9": "tanzania",
            "10": "vietnam", "11": "kyrgyzstan", "13": "israel", "14": "hongkong",
            "15": "poland", "16": "england", "17": "madagascar", "18": "dcongo",
            "19": "nigeria", "20": "macao", "21": "egypt", "22": "india",
            "23": "ireland", "24": "cambodia", "25": "laos", "26": "haiti",
            "27": "ivory", "28": "gambia", "29": "serbia", "31": "southafrica",
            "32": "romania", "33": "colombia", "34": "estonia", "35": "azerbaijan",
            "36": "canada", "37": "morocco", "38": "ghana", "39": "argentina",
            "40": "uzbekistan", "41": "cameroon", "42": "chad", "43": "germany",
            "44": "lithuania", "45": "croatia", "46": "sweden", "48": "netherlands",
            "49": "latvia", "50": "austria", "51": "belarus", "52": "thailand",
            "53": "saudiarabia", "54": "mexico", "55": "taiwan", "56": "spain",
            "58": "algeria", "59": "slovenia", "60": "bangladesh", "61": "senegal",
            "63": "czech", "64": "srilanka", "65": "peru", "66": "pakistan",
            "67": "newzealand", "68": "guinea", "70": "venezuela", "71": "ethiopia",
            "72": "mongolia", "73": "brazil", "74": "afghanistan", "75": "uganda",
            "76": "angola", "77": "cyprus", "78": "france", "79": "papua",
            "80": "mozambique", "81": "nepal", "82": "belgium", "83": "bulgaria",
            "84": "hungary", "85": "moldova", "86": "italy", "87": "paraguay",
            "88": "honduras", "89": "tunisia", "90": "nicaragua", "91": "timorleste",
            "92": "bolivia", "93": "costarica", "94": "guatemala", "97": "puertorico",
            "99": "togo", "100": "kuwait", "101": "salvador", "103": "jamaica",
            "104": "trinidad", "105": "ecuador", "106": "swaziland", "107": "oman",
            "108": "bosnia", "109": "dominican", "112": "panama", "114": "mauritania",
            "115": "sierraleone", "116": "jordan", "117": "portugal", "118": "barbados",
            "119": "burundi", "120": "benin", "123": "botswana", "128": "georgia",
            "129": "greece", "130": "guineabissau", "131": "guyana", "134": "saintkitts",
            "135": "liberia", "136": "lesotho", "137": "malawi", "138": "namibia",
            "140": "rwanda", "141": "slovakia", "142": "suriname", "143": "tajikistan",
            "145": "bahrain", "146": "reunion", "147": "zambia", "148": "armenia",
            "152": "burkinafaso", "154": "gabon", "155": "albania", "156": "uruguay",
            "157": "mauritius", "158": "bhutan", "159": "maldives", "160": "guadeloupe",
            "161": "turkmenistan", "162": "frenchguiana", "163": "finland",
            "164": "saintlucia", "165": "luxembourg", "166": "saintvincentgrenadines",
            "167": "equatorialguinea", "168": "djibouti", "169": "antiguabarbuda",
            "171": "montenegro", "172": "denmark", "173": "switzerland", "174": "norway",
            "175": "australia", "179": "aruba", "183": "northmacedonia",
            "184": "seychelles", "185": "newcaledonia", "186": "capeverde",
            "201": "gibraltar"
        }
        return jsonify(countries)

    if action not in ['getPrices', 'getServer']:
        return jsonify({"error": "Invalid action. Must be 'getPrices', 'getServer', or 'getCountries'."}), 400

    country_param = request.args.get('country', '22')
    remote_url = (
        f"http://api1.5sim.net/stubs/handler_api.php?"
        f"country={country_param}&api_key=d74c46dd007f4940bd37af35b8f39b64&action=getPrices"
    )

    # Run the asynchronous fetch using asyncio.run
    data = asyncio.run(fetch_with_retry(remote_url))
    if data is None:
        return jsonify({"error": "Failed to fetch data from remote API"}), 500

    # Check if the API returned an error response structure.
    if isinstance(data, dict) and "status" in data and "msg" in data:
        return jsonify({"error": data.get("msg", "Unknown error from API")}), 500

    transformed_data, selected_servers = transform_json_structure(data, country_mapping)
    return jsonify(transformed_data if action == 'getPrices' else selected_servers)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8443, debug=True)

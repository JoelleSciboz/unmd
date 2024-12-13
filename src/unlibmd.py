import json
import requests
import time
import xml.etree.ElementTree as ET
import importlib
import xmltodict
from lxml import etree

# GET UNDL API KEY
def get_key(path="../exclude/keys.json"):
    """
    Retrieves the 'undl_api_key' from a JSON file.

    Args:
        path (str): The path to the JSON file containing the credentials.
                    Defaults to '../exclude/keys.json' if not provided.

    Returns:
        str: The 'undl_api_key' value extracted from the JSON file.

    Raises:
        FileNotFoundError: If the specified JSON file is not found.

    """
    try:
        # Open the JSON file at the specified path
        with open(path) as f:
            # Load the JSON data into a dictionary
            credentials = json.load(f)
            # Retrieve and return the 'undl_api_key' from the dictionary
            return credentials['undl_api_key']
    except FileNotFoundError:
        # Raise an exception if the specified file is not found
        raise FileNotFoundError("JSON file not found at the specified path.")

# --- FUNCTION TO RETRIEVE AND MANIPULATE UNDL RECORDS IN MARC XML
# GET XML RECORDS FROM UNDL
def get_records_xml(parameters, api_key=None, check=1000):
    """
    Fetches all records from the UN Digital Library API.

    Args:
        params (dict): The parameters to be sent with the API request.
        api_key (str): The API key for authentication.
        check (int, optional): Number of records to fetch before printing status. Defaults to 1000.

    Returns:
        xml.etree.ElementTree.ElementTree: The XML tree containing all the records.
    """

    # Initialize variables
    url = "https://digitallibrary.un.org/api/v1/search?"
    search_id = None
    total = None
    root = ET.Element("collection")
    all_records = []
    if api_key is None:
        api_key = get_key()

    # Fetch records in a loop until there are no more records
    while True:
        if search_id:
            # If search_id exists, add it to the request parameters
            params['search_id'] = search_id
        else:
            # If search_id doesn't exist, create a copy of the original params
            params = parameters.copy()

        # Make the HTTP GET request to fetch the MARC XML data
        r = requests.get(url,
                         params=params,
                         headers={
                             "content-type": "application/xml",
                             "Authorization": "Token {}".format(api_key)
                         })
        
        # Check if the request was successful
        if r.status_code != 200:
            if r.status_code == 429:
                print("Let's wait 5 minutes! Too many requests, they say!")
                time.sleep(300)  # Wait for 5 minutes
                print("Let's try again!")
                r = requests.get(url,
                         params=params,
                         headers={
                             "content-type": "application/xml",
                             "Authorization": "Token {}".format(api_key)
                         })
            else:
                print(f"Error: Received status code {r.status_code}")
                break

        # Parse the response XML and remove the namespace
        try:
            response = ET.fromstring(
                r.text.replace('xmlns="http://www.loc.gov/MARC21/slim"', ''))
        except ET.ParseError as e:
            print("ParseError:", e)
            print("Response text:", r.text)
            break

        # Get the search_id from the response
        search_id = response.find('search_id').text
        
        # Get and print total number of records if total = None
        if not total:
            total = response.find('total').text
            print("Total nb. of records: " + total)

        # Find the collection element in the response
        collection = response.find('collection')

        # If there are no records in the collection, exit the loop
        if not collection:
            break

        # Append each record to the list of all records
        for record in collection.findall("record"):
            all_records.append(record)
        
        # Print the number of records fetched
        if len(all_records) % check == 0:
            print("Nb. of records processed: " + str(len(all_records)))

    # Append all the records to the root element
    for record in all_records:
        root.append(record)
    # Create a new XML tree with the root element
    new_xml_tree = ET.ElementTree(root)

    # Return the XML tree containing all the records
    return new_xml_tree

# CONVERT XML_TREE RETURNED BY `get_records_xml` INTO A LIST OF RECORDS

def convert_lxml(xml_et):
    """
    Converts an lxml ElementTree to a list of 'record' elements.

    Args:
        xml_et (etree._ElementTree): The input XML ElementTree.

    Returns:
        list: A list of 'record' elements found in the XML.
    """
    xml_string = ET.tostring(xml_et.getroot(), encoding='utf-8')
    xml_root = etree.fromstring(xml_string)
    xml_tree = etree.ElementTree(xml_root)
    records = xml_tree.findall('record')
    return records


# EXTRACT AND CONVERT SELECTED MARC XML ELEMENTS INTO A DICTIONARY
def extract_xml(record, elements):
    """
    Extracts data from an XML record and organizes it into a dictionary.

    Parameters:
        record (Element): An XML element representing the record to extract data from.
        elements (list): A list of dictionaries where each dictionary contains:
                        - field: Marc field number.
                        - element: Type of metadata element to extract: field, subfield.
                        - code: Subfield code of the metadata element to extract (e.g., 'a', 'b', 'c').
                        - name: Key name to use for the metadata element in the returned dictionary.

    Returns:
        dict: A dictionary with keys as specified in `elements` and values extracted
              from the XML record.
    """
    # Initialize the dictionary to store extracted data.
    dictionary_record = {}

    # Transform each element in the `elements` list into a query structure using `get_query`.
    # This prepares the XPath or other querying mechanism for extracting the data.
    elements = [get_query(e) for e in elements]

    # Extract the unique identifier (e.g., "001" field) from the control field.
    # Store it in the dictionary under the key "undl_id".
    control_field = record.find("controlfield[@tag='001']")
    dictionary_record["undl_id"] = control_field.text if control_field is not None else None

    # Iterate through each metadata element definition in `elements`.
    for element in elements:
        # Use the query to locate matching XML elements in the record.
        xml_element = record.findall(element["query"])

        # Case 1: Exactly one matching element is found.
        if len(xml_element) == 1:
            if element["element"] == "field":
                # For "field" elements, extract subfield code-value pairs.
                v = [{e.get("code"): e.text for e in xml_element[0].xpath(".//subfield")}]
            else:
                # Otherwise, use the text content of the single matching element.
                v = [xml_element[0].text]

        # Case 2: Multiple matching elements are found.
        elif len(xml_element) > 1:
            if element["element"] == "field":
                # For "field" elements, extract subfield code-value pairs for all matches.
                code_value_list = []
                for datafield in xml_element:
                    code_value_list.extend(
                        [{e.get("code"): e.text for e in datafield.xpath(".//subfield")}]
                    )
                v = code_value_list
            else:
                # For other elements, collect text content from all matches.
                v = [e.text for e in xml_element]

        # Case 3: No matching elements are found.
        else:
            # If no matches are found, set the value to None.
            v = None

        # Store the extracted value in the dictionary using the specified key name.
        dictionary_record[element["name"]] = v

    # Return the dictionary containing all extracted data.
    return dictionary_record


## CONSTRUCT XPATH TO RETRIEVE MARC XML ELEMENTS
def get_query(element):
    """
    Constructs an XPath query string based on the input dictionary 'element' and updates the dictionary with the query.

    Parameters:
    element (dict): A dictionary containing the following keys:
        - 'field' (str): Represents the datafield tag.
        - 'code' (str): Represents the subfield code.
        - 'ind1' (str or None): Represents the first indicator, if provided.

    Returns:
    dict: The input dictionary updated with a new key 'query', containing the constructed XPath query string.
    """
    # Extract the 'field' value from the dictionary; this is mandatory
    field = element["field"]
    
    # Extract the 'code' value, which may be an empty string or None
    code = element["code"]
    
    # Extract the 'ind1' value, which may be None
    ind1 = element["ind1"]
    
    # Initialize the query string with the datafield tag, using the 'field' value
    query = f"datafield[@tag='{field}']"
    
    # If 'ind1' is provided (not None), append it as an attribute to the query string
    if ind1:
        query += f"[@ind1='{ind1}']"
    
    # If 'code' is provided (including an empty string), append the subfield code to the query string
    # The condition allows 'code' to be an empty string but excludes None
    if code or code == "":
        query += f"/subfield[@code='{code}']"
    
    # Add the constructed query string to the dictionary under the key 'query'
    element["query"] = query
    
    # Return the updated dictionary
    return element


# --- FUNCTION TO RETRIEVE UNDL RECORDS AS JSON RATHER THAN MARC XML
def undl_request(parameters, result_queue=None):
    """
    Makes a request to the UN Digital Library API and retrieves results in XML format.
    
    Parameters:
    parameters (dict): A dictionary of parameters to be sent in the API request.
    result_queue (queue.Queue): A queue to store the result of the function.

    Returns:
    tuple: (log, records), records will be empty if no records are retrieved.
    """
    
    # Initialize all variables
    log = [] # empty list to store the logs
    records = [] # empty list to store the records
    status = None
    total = None
    search_id = None
    error = None
    
    # Add format as 'xml' to the request parameters
    parameters["format"] = "xml"
    
    # Define the base URL for the API request
    url = "https://digitallibrary.un.org/api/v1/search"
    
    # Retrieve the API key
    key = get_key()
    
    # Make the API request with the provided parameters and authorization header
    try:
        retry_count = 0
        
        # Retry up to 2 times if the request fails
        while retry_count < 2:
            # Send the GET request to the API
            r = requests.get(url, params=parameters, headers={"Authorization": "Token " + key}, stream=True)
            status = r.status_code
            
            # Check if the status code is 429 (Too Many Requests)
            if status == 429:
                # Handle too many requests error by waiting and retrying
                error = "Let's wait 5 minutes! Too many requests, they say!"
                print(error)
                retry_count += 1
                time.sleep(300)  # Wait for 5 minutes before retrying
                print("Let's try again...")
                continue  # Skip the rest of the loop and retry
            
            # Check if the status code is not 200 (OK)
            elif status != 200:
                # Parse the error message from the response
                try:
                    r_dict = r.json()
                except ValueError:
                    error = f"{status}: Unable to parse error message"
                break  # Exit the retry loop
            
            # If the request was successful
            elif status == 200:
                # Parse the XML response
                r_dict = xmltodict.parse(r.text, encoding='UTF-8')
                
                # Extract the total number of results and search ID from the response
                total = int(r_dict['response']['total'])
                try:
                    search_id = r_dict['response']['search_id']
                except KeyError:
                    print("No records, check your search parameters!")
                
                # Check if records are present in the response
                if 'record' in r_dict['response']['collection']:
                    records = r_dict['response']['collection']['record']
                else:
                    error = f"{status}: No records"
                break  # Exit the retry loop
    
    except requests.exceptions.RequestException as e:
        # Handle any exceptions raised during the request
        error = str(e)
    
    # Log the request details
    log = [status, total, len(records), error, search_id]
    
    # Return a tuple containing the log and records
    result = (log, records)
    
    # If a result queue is provided, put the result in the queue
    if result_queue is not None:
        result_queue.put(result)
    
    return result


def get_records_json(parameters, check=1000):
    """
    Fetches all records from the UN Digital Library API based on the provided parameters.

    Parameters:
    parameters (dict): A dictionary of parameters to be sent in the API request.

    Returns:
    tuple: (logs, all_records)
        logs (list): A list of logs for each request made.
        all_records (list): A list of all records retrieved.
    """
    
    # Make a local copy of the parameters to avoid modifying the original
    search_parameters = parameters.copy()
    
    # Initialize variables to store all records, total number of records, and logs
    all_records = []
    total = None
    logs = []
    
    # Loop until all records are retrieved
    while len(all_records) != total:
        # Make the API request and retrieve the log and records
        log, records = undl_request(search_parameters)
        
        # Extend the list of all records with the new records retrieved
        all_records.extend(records)
        
        # Append the log of this request to the logs list
        logs.append(log)
        
        # If total is not set, retrieve and print the total number of records
        if total is None:
            total = log[1]
            print("Total number of records: " + str(total))
            
            # Update the search parameters to include the search_id for subsequent requests
            search_parameters["search_id"] = log[-1]
        
        # Print the number of records processed every 1000 records
        if len(all_records) % check == 0:
            print("Number of records processed: " + str(len(all_records)))
    
    # Return the logs and all records retrieved
    return logs, all_records


### --- UTILITIES ---

## ADD LINKS USING AN ID AND A TEMPLATE

def add_links(column_value, template):
    """
    Concatenates a value with a provided template string to create a link

    Args:
        column_value (str): The value from the column that will be appended to the template.
        template (str): The string template to prepend to the column value.

    Returns:
        str: A concatenated string combining the template and the column value.
    
    Example:
        add_links("123", "https://example.com/item/") 
        -> "https://example.com/item/123"
    """
    # Combine the template with the column value.
    # This function assumes both inputs are strings and will concatenate them directly.
    return template + str(column_value)

## CONVERT MD ID - REMOVE (DHL) PREFIX FROM UNDL 035

def convert_me_id(column_value):
    """
    Extracts and returns the first value from a list that starts with '(DHL)',
    with the '(DHL)' prefix removed and the result stripped of leading/trailing whitespace.

    If the input is not a list, the function returns the original input.

    Args:
        column_value (list or any): The input value, expected to be a list or any other type.

    Returns:
        str or original input: The modified value if a match is found in the list; otherwise, 
                               the original input is returned.
    """
    if isinstance(column_value, list):
        for value in column_value:
            # Ensure the value is a string and starts with '(DHL)'
            if isinstance(value, str) and value.startswith('(DHL)'):
                return value.replace('(DHL)', '').strip()  # Remove prefix and strip whitespace
        return None  # Return None if no match is found
    return column_value  # Return the original value if input is not a list


## REMOVE SUBFIELD KEYS AND CONCATENATE VALUES IN A FIELD AND CONCATENATE
def flatten(column_value):
    """
    Flattens a list of dictionaries by extracting and concatenating all dictionary values 
    into a string. If the input is not a list, it returns the original input.

    Args:
        column_value (list or any): The input value, expected to be a list of dictionaries or other types.

    Returns:
        list or original input: A list of concatenated strings for each dictionary in the input list, 
                                or the original input if not a list.
    """
    flatten_field = []
    if isinstance(column_value, list):
        for value in column_value:
            if isinstance(value, dict):
                # Convert all dictionary values to strings and join them with spaces
                value_string = ' '.join(str(value) for value in value.values())
                flatten_field.append(value_string)
        return flatten_field  # Return the flattened list of strings
    else:
        return column_value  # Return the original input if not a list


## REMOVE LIST FROM COLUMN RETURN ONE STRING, MULTIPLE VALUES ARE SEPARATED WITH |
def clean(column_value):
    """
    Cleans a list by converting its elements to strings and joining them with a comma.
    If the input is not a list, it returns the original input.

    Args:
        column_value (list or any): The input value, expected to be a list or any other type.

    Returns:
        str or original input: A comma-separated string if the input is a list, 
                               or the original input if not a list.
    """
    
    if isinstance(column_value, list):
        # Convert each element of the list to a string and join with commas
        return '|'.join(map(str, column_value))
    else:
        return column_value  # Return the original input if not a list

## EXTRACT A PARTICULAR SUBFIELD FROM A FIELD COLUMN
def extract(column_value, subfield):
    """
    Extract values from a list of dictionaries based on a specified subfield.

    Args:
        column_value (list): A list of dictionaries to search for the subfield.
        subfield (str): The key to look for in each dictionary.

    Returns:
        list or None: A list of extracted values if the subfield is found in any dictionary.
                      Returns None if there are no values to extract or if the input is not a list.

    Example:
        column_value = [{'a': 1}, {'b': 2}]
        subfield = 'a'
        extract(column_value, subfield) -> [1]
    """
    # Check if the input is a list
    if isinstance(column_value, list):
        extracted_values = []  # Initialize an empty list to store extracted values

        # Iterate through each item in the list
        for value in column_value:
            # Check if the item is a dictionary and contains the specified subfield
            if isinstance(value, dict) and subfield in value:
                # Append the value associated with the subfield to the extracted values list
                extracted_values.append(value[subfield])

        # Return the list of extracted values if not empty; otherwise, return None
        return extracted_values if extracted_values else None

    # Return None if the input is not a list
    return None

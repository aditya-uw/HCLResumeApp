"""
coding=utf-8
Code Template
"""
import inspect
import logging
import os
import sys
import re
import yaml
import boto3
import json
from ast import literal_eval

CONFS = None
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
AVAILABLE_EXTENSIONS = {'.csv', '.doc', '.docx', '.eml', '.epub', '.gif', '.htm', '.html', '.jpeg', '.jpg', '.json',
                        '.log', '.mp3', '.msg', '.odt', '.ogg', '.pdf', '.png', '.pptx', '.ps', '.psv', '.rtf', '.tff',
                        '.tif', '.tiff', '.tsv', '.txt', '.wav', '.xls', '.xlsx'}
LAMBDA_TASK_ROOT = os.environ.get('LAMBDA_TASK_ROOT', os.path.dirname(os.path.abspath(__file__)))
DYNAMODB_TABLE = os.environ.get('DYNAMO_DB_TABLE',"resumewordcloudTable")

class ExtraHeader:
    a = "test"

header = ExtraHeader()

def lambda_handler(event, context):
    """
    Main function documentation template
    :return: None
    :rtype: None
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE)
    key = event['Records'][0]['Sns']['Message'];
    response = table.get_item(Key={'name': key})
    item = response['Item']
    text = item['text']
    # Extract data from upstream.
    #observations = extract(key, text)
    extract(key, text)
    #print(observations.loc[0])
    #print(observations.to_json())
    s = json.dumps(header.__dict__) 
    item['extraHeader'] = s
    table.put_item(Item=item)
    print("UpdateItem succeeded:")
    
    pass


# Extracts all the information that can be extracted and stores as the fields of an object
def extract(key, text):
    # Reference variables
    #candidate_file_agg = list()
    canName, totalstr = extract_ids(text)
    keys = extract_keys(totalstr)
    # Convert list to a pandas DataFrame
    header.file_path = key
    header.extension = os.path.splitext(header.file_path)[1]
    header.text = totalstr
    header.candidate_name = canName
    # Extract contact fields
    header.email = extract_email(header.text)
    header.phone = extract_phone(header.text)
    header.years_of_experience = extract_years_of_experience(header.text)
    # header.bolded = bolds
    # header.italics = italics
    header.work_info = extract_work_info(keys, header.text)
    header.university_info = extract_university_info(header.text)
    header.awards_achievements_accomplishments = extract_awards(header.text)
    # Extract skills
    extract_fields()


# Extracts information of candidate awards using regexes and keywords
def extract_awards(text):
    currentRegex = r"(^.*\W*(?:Awards?|Achievements?|Patents?|awards?|achievements?|patents?)\W*.*$)"
    return term_match(text, currentRegex)

# Extracts information of candidate university using regexes and keywords
def extract_university_info(text):
    currentRegex = r"(^.*\W*(?:University|university?|college?|College?|Institute?|institute?)\W*.*$)"
    return term_match(text, currentRegex)
    
# Extracts information of candidate's years of experience using regexes and keywords
def extract_years_of_experience(text):
    currentRegex = r"(^.*\W*(?:experience|EXPERIENCE?|Experience)\W*.*$)"
    sentences = term_match(text, currentRegex)
    sentence = sentences[0]
    sentence = sentence.replace('\n', ' ')
    sentence = sentence.replace('\t', ' ')
    currentRegex = r"([0-9][0-9]\W*years)"
    return term_match(sentence, currentRegex)[0]

# Extracts information of candidate's email using regexes
def extract_email(text):
    currentRegex = r"[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9]"
    currentRegex += "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    currentRegex += "(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    return term_match(text, currentRegex)

# Extracts information of candidate's phone number using regexes
def extract_phone(text):
    currentRegex = r"(\+?.\d{1,2}[\s.-])?(?:\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}?|\d{5}[\s.-]\d{5})"
    return term_match(text, currentRegex)
   
# Extracts information of candidate's specific work experiences using regexes
def extract_keys(totalstr):
    currentRegex = r"\b^.*(?:Jan(?:uary?|’?|'?)?"
    currentRegex += "|Feb(?:ruary?|’?|'?)?|Mar(?:ch?|’?|'?)?|Apr(?:il?|’?|'?)?"
    currentRegex += "|May?|June?|July?|Aug(?:ust?|’?|'?)?|Sep(?:tember?|’?|'?)?"
    currentRegex += "|Oct(?:ober?|’?|'?)?|Nov(?:ember?|’?|'?)?"
    currentRegex += "|Dec(?:ember?|’?|'?)?|Fall?|Spring?|Autumn?|Summer?"
    currentRegex += "|Winter?|[1-2][8-9]?).[0-9][0-9].*$"
    keys = term_match(totalstr, currentRegex)
    currentRegex =r"\b^.*(?:\d{2}/\d{2}?|Now?|now?|Present?|present?|Today?|today)"
    currentRegex += " - (?:\d{2}/\d{2}?|Now?|now?|Present?|present?|Today?|today).*$"
    other_keys = term_match(totalstr, currentRegex)
    if len(other_keys) > 0:
        keys = keys + other_keys
    return keys

# Extracts a secondary name in case regex does nto work from the first line of the text
# Also cleans up each line of the text to enable cleaner extraction
def extract_ids(text):
    lineNum = 0
    totalstr = ""
    boldItalicsContent = ''
    for line in text:
        if '[\'' in line:
            boldItalicsContent = boldItalicsContent + line
        else:
            if lineNum == 0:
                backupCanName = extract_backup_name(line)
                totalstr += line    
            else:
                totalstr += remove_start_tab(line)
        lineNum = lineNum + 1
    # boldContent = boldItalicsContent[:boldItalicsContent.index('\']')+2]
    # italicsContent = boldItalicsContent.replace(boldContent, '')
    # bolds = extract_bolds(boldContent)
    # italics = extract_italics(italicsContent)
    totalstr = totalstr.replace('    ', ' ')
    totalstr = totalstr.replace('   ', ' ')
    totalstr = totalstr.replace('  ', ' ')
    canName = backupCanName
    return canName, totalstr

# Extracts all bolded content in the text
def extract_bolds(boldContent):
    bolds = []
    for bold in literal_eval(boldContent):
        if '\t' in bold:
            bold = remove_string('\t', bold)
        if len(bold) > 2:
            bolds.append(bold)
    return bolds

# Extracts all italicized content from the text
def extract_italics(italicsContent):
    italics = []
    for italic in literal_eval(italicsContent):
        if '\t' in italic:
            italic = remove_string('\t', italic)
        if len(italic) > 2:
            italics.append(italic)
    return italics

# Extracts complete work_info and puts it into a set of organized information
def extract_work_info(keys, totalstr):
    organized_info = []
    for key in keys:
        key = key.strip()
        disorganized_work_dict = {'years': '', 'information': ''}
        disorganized_work_dict['years'] = extract_years(key)
        disorganized_work_dict['information'] = extract_information(key, disorganized_work_dict['years'])
        info = clean_up(key, totalstr, disorganized_work_dict['information'], disorganized_work_dict['years'])
        years = disorganized_work_dict['years']
        disorganized_work_dict['information'] = info
        work_filter = (len(years) > 9 or len(years) == 4) and len(info) > 26
        if work_filter:
            organized_info.append(disorganized_work_dict)
    return organized_info
       
# Removes the tab at the start of any line
def remove_start_tab(line):
    if '\t' in line[0:2]:
        return line[0:2].replace('\t', '') + line[2:]
    else:
        return line

# Extracts the words at the start of the line and assumes it as a name
def extract_backup_name(line):
    if '\t' in line:
        esc_index = line.index('\t')
        return line[:esc_index]
    elif '\n' in line:
        esc_index = line.index('\n')
        return line[:esc_index]
    else:
        return line

# Cleans up extracted information based on how it is structured and reorganizes it to match formatting
def clean_up(key, text, info, years):
    if (info == years) or (info == ''):
        info = extract_around_key(text, key)
        #print("DISORGANIZED INFORMATION:", info)   
    info = remove_unnecessary(info)
    #info = remove_string('\x9f', info)
    #info = remove_string('\uf0a7', info)
    return info

# Removes unnecessary spaces and tabs in between words
def remove_unnecessary(info):
    if '  ' in info[0:int(len(info)/2)]:
        space_position = info[0:int(len(info)/2)].index('  ')
        space_sentence = info[space_position:]
        space_sentence = space_sentence.strip()
        info = space_sentence
    if '  ' in info:
        space_position = info.index('  ')
        space_sentence = info[:space_position]
        space_sentence = space_sentence.strip()
        info = space_sentence
    return info
       
# Extracts the words before and after a specific key to get the information that is relevant to that key
def extract_around_key(text, key):
    key_position = text.find(key)
    key_sentence = text[key_position - 45:key_position + len(key) + 45]
    key_sentence = key_sentence.replace('\n', ' ')
    key_sentence = key_sentence.strip()
    return key_sentence

# Extracts information such as employer and location that is relvant to work experience
def extract_information(key, years):
    if len(years) > 0:
        key_position = key.find(years.split()[0])
        pre_info = key[:key_position]
        post_info = key[key_position:]
        if len(pre_info) < 15:
            return post_info
        elif len(post_info) < 15:
            return pre_info
        else:
            return key
    else:
        return ''

# Extracts the years of work experience for a candidate using regexes and methods to clean up and organize
def extract_years(key_string):
    currentRegex = r"\b(?:(?:Jan(?:uary?|’?|'?)?|Feb(?:ruary?|’?|'?)?"
    currentRegex += "|Mar(?:ch?|’?|'?)?|Apr(?:il?|’?|'?)?|May(?:y?|’?|'?)?"
    currentRegex += "|Jun(?:e?|’?|'?)?|Jul(?:y?|’?|'?)?|Aug(?:ust?|’?|'?)?"
    currentRegex += "|Sep(?:tember?|’?|'?)?|Oct(?:ober?|’?|'?)?"
    currentRegex += "|Nov(?:ember?|’?|'?)?|Dec(?:ember?|’?|'?)?|Fall?|Spring?"
    currentRegex += "|Autumn?|Summer?|Winter?).(?:19[7-9]\d?|2\d{3}?|\d{2}?)?"
    currentRegex += "|Present?|present?)"
    disorganized_work_info = term_match(key_string, currentRegex)
    #print("DISORGANIZED_WORK_INFO: ", disorganized_work_info)
    for num in range(3):
        if len(disorganized_work_info) > 1:
            return str(disorganized_work_info[0]) + ' - ' + str(disorganized_work_info[1])
            #print("CURRENTREGEX: ", currentRegex)
        elif len(disorganized_work_info) == 1 and len(disorganized_work_info[0]) > 0:
                return str(disorganized_work_info[0])
                #print("CURRENTREGEX: ", currentRegex)
        else:
            if num == 0:
                currentRegex = r"\b(?:19[7-9]\d|20\d{2}?|[7-9]\d|present|Present$)"
            elif num == 1:
                currentRegex = r"\b(?:\d{2}/\d{2}?|Now?|now?|Present?|present?|Today?|today)"
            else:
                return ''
            disorganized_work_info = term_match(key_string, currentRegex)
            
def remove_string(s_t_r, text):
    '''
    A method to remove a specific substring in a larger string only if it exists
    '''
    if s_t_r in text:
        text = text.replace(s_t_r, '')

def term_match(string_to_search, term):
    """
    A utility function which return the first match to the `regex_pattern` in the `string_to_search`
    :param string_to_search: A string which may or may not contain the term.
    :type string_to_search: str
    :param term: The term to search for the number of occurrences for
    :type term: str
    :return: The first match of the `regex_pattern` in the `string_to_search`
    :rtype: str
    """
    try:
        matches = re.finditer(term, str(string_to_search), re.MULTILINE)
        result = []
        for matchNum, match in enumerate(matches, start=1):
            result.append(match.group().replace("\t", " "))
        if len(result) > 0:
            return result
        else:
            return ['']
    except Exception:
        return None
    
def term_count(string_to_search, term):
    """
    A utility function which counts the number of times `term` occurs in `string_to_search`
    :param string_to_search: A string which may or may not contain the term.
    :type string_to_search: str
    :param term: The term to search for the number of occurrences for
    :type term: str
    :return: The number of times the `term` occurs in the `string_to_search`
    :rtype: int
    """ 
    try:
        regular_expression = re.compile(term, re.IGNORECASE)
        result = re.findall(regular_expression, str(string_to_search))
        return len(result)
    except Exception:
        logging.error('Error occurred during regex search')
        return 0

# Extract fields and skills of the candidate
def extract_fields():
    header.extra_fields = {}
    for extractor, items_of_interest in get_conf('extractors').items():
        extra_skills = extract_skills(header.text, extractor, items_of_interest)
        if (len(extra_skills) != 0):
            header.extra_fields[extractor] = extra_skills

# Extract all skills and relevant information depending on the items of interest for the employer
def extract_skills(resume_text, extractor, items_of_interest):
    potential_skills_dict = dict()
    matched_skills = []
    # TODO This skill input formatting could happen once per run, instead of once per observation.
    for skill_input in items_of_interest:

        # Format list inputs
        if type(skill_input) is list and len(skill_input) >= 1:
            potential_skills_dict[skill_input[0]] = skill_input

        # Format string inputs
        elif type(skill_input) is str:
            potential_skills_dict[skill_input] = [skill_input]
        else:
            logging.warn('Unknown skill listing type: {}. Please format as either a single string or a list of strings'
                         ''.format(skill_input))
    for (skill_name, skill_alias_list) in potential_skills_dict.items():

        skill_matches = 0
        # Iterate through aliases
        for skill_alias in skill_alias_list:
            # Add the number of matches for each alias
            skill_matches += term_count(resume_text, skill_alias.lower())
        # If at least one alias is found, add skill name to set of skills
        if skill_matches > 0:
            matched_skills.append(skill_name)
    return matched_skills

# confs is a file that stores a a bunch of keywords that raises employer attention
# Such as outstanding university or companies
def load_confs(confs_path=LAMBDA_TASK_ROOT + '/config.yaml'):
    # TODO Docstring
    global CONFS

    if CONFS is None:
        try:
            CONFS = yaml.load(open(confs_path))
        except IOError:
            confs_template_path = confs_path + '.template'
            logging.warn(
                'Confs path: {} does not exist. Attempting to load confs template, '
                'from path: {}'.format(confs_path, confs_template_path))
            CONFS = yaml.load(open(confs_template_path))
            
    return CONFS


def get_conf(conf_name):
    return load_confs()[conf_name]
    

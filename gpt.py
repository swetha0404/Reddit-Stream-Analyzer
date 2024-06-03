import os
from openai import AzureOpenAI
import json

text_file_path = f"./reddit_data.txt"
prompt_path = f"./user_prompt.txt"
output_dir = f"./summarized_results.json"

def read_config(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            config[key] = value
    return config

# Read configuration from the file
config_file_path = "openai_config.txt"

def read_contents(file_path):
    # Open the text file and read its contents
    with open(file_path, 'r',encoding='latin-1') as file:
        content = file.read()

    # Return the content read from the file
    return content

def add_to_prompt(contents, prompt_path):
    # Read the contents of the text file
    with open(prompt_path, 'r',encoding='latin-1') as file:
        file_content = file.read()
    # Find the index where "Paragraphs to summarise" section ends
    index = file_content.find("Paragraphs to summarise") + len("Paragraphs to summarise")
    # If there's content below "Paragraphs to summarise", erase it
    if index < len(file_content):
        file_content = file_content[:index]
    # Write the updated content back to the file
    with open(prompt_path, 'w',encoding='latin-1') as file:
        file.write(file_content)

def write_to_json(contents, summary, output_dir):
    """Write contents and summary to a JSON file with unique IDs."""
    os.makedirs(output_dir, exist_ok=True)
    serial_number = 1
    while os.path.exists(os.path.join(output_dir, f"data_{serial_number}.json")):
        serial_number += 1
    data = {
        "text": contents,
        "summary": summary
    }
    output_file = os.path.join(output_dir, f"data_{serial_number}.json")
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    print(f"Data written to: {output_file}")

def chatgpt_interaction(prompt_path):
    config = read_config(config_file_path)
    client = AzureOpenAI(
        api_key=config["API_KEY"],
        api_version=config["API_VERSION"],
        azure_endpoint=config["AZURE_ENDPOINT"]
    )
    messages = []
    
    with open(prompt_path, 'r',encoding='latin-1') as file:
        prompt_content = file.read()
    messages = [{'role': 'user', 'content': prompt_content}]
    response = client.chat.completions.create(
        model="gpt-35",
        messages=messages,
        temperature=0.7,
    )
    assistant_response = response.choices[0].message.content
    messages.append({'role': 'assistant', 'content': assistant_response})
    return assistant_response

def text_summarize(text_file_path, prompt_path):
    contents = read_contents(text_file_path)
    add_to_prompt(contents, prompt_path)
    gpt_response = chatgpt_interaction(prompt_path)
    print("--------------------------------------------------------------------------------------------------")
    print(gpt_response)
    print("--------------------------------------------------------------------------------------------------")
    write_to_json(contents, gpt_response, output_dir)
    return(gpt_response)
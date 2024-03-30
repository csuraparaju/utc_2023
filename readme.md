### Set up
- Clone the repository
- (Optional) Create a virtual environment called utc24
    - `python3 -m venv utc24`
    - `source utc24/bin/activate`
- Install the requirements
    - `pip install xchangelib`
    - `pip install dotenv`

### Running the script
- Create a .env file in the root directory of the project
    - Add the following variables to the .env file
        - `SERVER=staging.uchicagotradingcompetition.com:3333`
        - `USERNAME=username-given-to-us-by-utc`
        - `PASSWORD=password-given-to-us-by-utc`
- Run the bots
    - `python3 case1/{bot_name}.py`



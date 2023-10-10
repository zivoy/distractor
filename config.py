import configparser

config = configparser.ConfigParser()
config.read_string("""
[SETTINGS]
OutputLocation = ./
""")

config.read("config.ini")
settings = config["SETTINGS"]

assert("OutputLocation" in settings)
assert("SourceFolder" in settings or "SourceZip" in settings)


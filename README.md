# Custom exporter to MicroStrategy

## First time setup
#### Plugin settings
Head to the plugin's settings and input the URL to the API endpoint of your MicroStrategy endpoint. This URL ends with ""/MicroStrategyLibrary/api".

#### Parameter set: API credentials
Define sets of username and password to be used by your users.

#### Parameter set: project name
Define sets of project names to be used by your users.

## Usage
Use the "Export" functionality on your dataset, click on the "Custom" tab and select MicroStrategy.
You will be asked to select:
- A preset for API credentials. You can input another username and password manually at this stage.
- A preset for the MicroStrategy project name. You can set a preset for the current project in project settings.
- The name you wish to give to your dataset (also called cube in MicroStrategy)

After export, you can see your cube in the MicroStrategy interface with the string " (created by Dataiku DSS)" added to its name.

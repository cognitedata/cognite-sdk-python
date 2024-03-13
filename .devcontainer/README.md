# Dev Container for Cognite Python SDK

The Dev Container setup in the `/.devcontainer` folder enables easy source navigation, development and debugging of the SDK using Visual Studio Code. The Dev Container can run in Github CodeSpaces or in a local (or remote) Docker environment.

You can choose to view the remotely running dev container in your browser or in VSCode running locally.

See more info about running in Github CodeSpaces at https://docs.github.com/en/codespaces/overview,
and for running in Docker at: https://code.visualstudio.com/docs/devcontainers/containers.

## Set up default editor

To open in VSCode make sure you have Visual Studio Code installed, with the Remote Development extension installed (will installed bu default).
Adjust the default editor for GitHub CodeSpaces in your github configuration, as documented in https://docs.github.com/en/codespaces/setting-your-user-preferences/setting-your-default-editor-for-github-codespaces.

## Start dev container
To start the devcontainer in Github CodeSpaces,
- Go to the repository at https://github.com/cognitedata/cognite-sdk-python
- Click the green `Code` button, and choose the `Codespaces` panel.
- Click the "Create codespace" or + (plus) sign to create a codespace with the default setup based on the master branch. Alternatively use the triple dot menu, where you can configure a custom setup, for example if more memory is needed.
- Wait for the devcontainer to start up and eventually see that a terminal session with the poetry python environment loaded.

If something fails, or you want to check what happens behind the scenes, you can check the creation log with CTRL+Shift+P -> "View creation log".

## Poetry 
Poetry install will be run as part of setup.
You can see that the poetry based python environment is used in the terminal window.

## Running and debugging tests
To run tests in VSCode test, use the `Testing` panel in the VSCode menu on the left.
You may need to click the Refresh tests button (curly arrow) in the Testing panel to make the tests be discovered.

Select a test, i.e. search for it, and choose to run it with the arrow in the text explorer, or in the source code. Add breakpoints, step through code, etc. The unit tests can be run without any further setup.

## Contributing

See more details about how to do development for the Cognite Python SDK in the CONTRIBUTING.md file.
To be able to run some of the integration tests you may need to set up a .env file with proper login flow settings, as described in that file.

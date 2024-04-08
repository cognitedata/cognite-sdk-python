# Dev Container for Cognite Python SDK

The Dev Container setup in the `/.devcontainer` folder enables easy source navigation, development and debugging of the SDK using Visual Studio Code. The Dev Container can run in Github CodeSpaces or in a local (or remote) Docker environment.

To use the Dev container through VSCode running locally make sure you have Visual Studio Code installed. <https://code.visualstudio.com/>.

Use one of these links to open the Dev Container with a default configuration on the master branch. For a customized setup, see further down in this README.

[![Open in GitHub Codespaces](https://img.shields.io/static/v1?label=Github%20Codespaces&message=Open&color=lightgreen&logo=github)](https://codespaces.new/cognitedata/cognite-sdk-python)

[![Open in Dev Container (Docker)](https://img.shields.io/static/v1?label=Dev%20Container%20in%20Docker&message=Open&color=blue&logo=docker)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/cognitedata/cognite-sdk-python)

See more info about running in Github CodeSpaces at <https://docs.github.com/en/codespaces/overview>,
and for running as Devcontainer in Docker at: <https://code.visualstudio.com/docs/devcontainers/containers>.

## Set up default editor

In GitHub Codespaces you can choose to view the remotely running dev container in your browser or in VSCode running locally.
You can adjust the default editor mode (browser or VSCode on your host OS) for GitHub CodeSpaces in your github configuration, as documented in <https://docs.github.com/en/codespaces/setting-your-user-preferences/setting-your-default-editor-for-github-codespaces>.

## Start custom dev container

To start the devcontainer in Github CodeSpaces, use the "Open in Github Codespaces" link above, or follow these manual steps.

- Go to the repository at <https://github.com/cognitedata/cognite-sdk-python>.
- Click the green `Code` button, and choose the `Codespaces` panel.
- Click the "Create codespace" or + (plus) sign to create a codespace with the default setup based on the master branch. Alternatively use the triple dot menu, where you can configure a custom setup, for example if more memory is needed.
- Wait for the devcontainer to start up and eventually see that a terminal session with the poetry python environment loaded.

To start the devcontainer in Docker, use the "Open in Dev Container (Docker)" link above, or follow these manual steps.

- Ensure you have Docker installed and running on your machine.
- Open VSCode.
- Open the Command Palette (Ctrl+Shift+P) and type `Dev Containers: Clone Repository in Named Container Volume`.
- Choose GitHub, and select `cognitedata/cognite-python-sdk` as the repository. Choose the branch, normally main or master.
- Click Create new volume. Select a proper name for the volume, or just use the suggested one.
- Enter a target folder name where the repository will be cloned. Click enter.

VSCode will then start cloning the repository in a new volume in your Docker setup. It will then start the dev container and install the necessary tools and dependencies.
A shell will open in the dev container, and you can start working with the code.

If something fails, or you want to check what happens behind the scenes, you can check the creation log with CTRL+Shift+P -> "View creation log".

## Poetry

Poetry install will be run as part of setup.
You can see that the poetry based python environment is used in the terminal window. The shell may have to be restarted to get the poetry environment activated.

## Running and debugging tests

To run tests in VSCode test, use the `Testing` panel in the VSCode menu on the left.
You may need to click the Refresh tests button (curly arrow) in the Testing panel to make the tests be discovered.

Select a test, i.e. search for it, and choose to run it with the arrow in the text explorer, or in the source code. Add breakpoints, step through code, etc. The unit tests can be run without any further setup.

## Contributing

See more details about how to do development for the Cognite Python SDK in the CONTRIBUTING.md file.
To be able to run some of the integration tests you may need to set up a .env file with proper login flow settings, as described in that file.

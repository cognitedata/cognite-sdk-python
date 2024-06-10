// Description: This script is used to test the Python SDK installation in a Pyodide environment.
// We are using JupyterLite and stlite, both pyodide based Python runtimes. To ensure that the SDK works
// in these runtimes, we need to test the installation of the SDK in a Pyodide environment.
// This script will start an HTTP server to serve the SDK wheel file and then try to install the SDK in Python.
// If the installation is successful, it will run a simple Python script to test the SDK.
const { loadPyodide } = require("pyodide");

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 3000;

// The Cognite Python SDK wheel filename will be sent in as environment variable
const wheelFilePath = path.join(__dirname, '..', 'dist', process.env.SDK_FILE_PATH);

// Create an HTTP server to serve the wheel file
const server = http.createServer((req, res) => {
  fs.readFile(wheelFilePath, (err, data) => {
    if (err) {
      // Handle file read errors
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('500 - Internal Server Error');
    } else {
      // Serve the file content
      res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
      res.end(data);
    }
  });
});

// Start the server and listen on the defined port. Then try to install the SDK in Python
server.listen(PORT, () => {
  console.log(`Server is running at http://localhost:${PORT}. Now trying to install sdk.`);
  
  async function test_cognite_sdk() {
    let pyodide = await loadPyodide();
    await pyodide.loadPackage("micropip");
    const micropip = pyodide.pyimport("micropip");
    // Read packages to install from environment variable as JSON

    const packages = JSON.parse(process.env.PACKAGES);
    for (const pkg of packages) {
      await micropip.install(pkg);
    }
    await pyodide.runPythonAsync("from cognite.client import CogniteClient");
    
    return pyodide.runPythonAsync("Python SDK successfully installed and imported!");
  }

  test_cognite_sdk().then((result) => {
    console.log("Response from Python =", result);
    server.close();
  });
});

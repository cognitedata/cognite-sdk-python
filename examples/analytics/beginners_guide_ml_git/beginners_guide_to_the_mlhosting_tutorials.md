<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-sdk-python/blob/master/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Beginners tips and tricks to the model hosting tutorials
===========================

A thorough description of the prerequisites required for the model hosting tutorials on Windows,
and in depth explanation of some lines of code from the tutorials which you can consult if needed.

## Prerequisites

###Access
- Obtain an API key and a project name from an administrator
- Add the API key as an environment variable for security purposes. I named it "apikeyml" in the example below

![](.beginners_guide_to_the_mlhosting_tutorials_images\4aa33ba6.png)

###Installation
In order to start the tutorials you will need a programming environment. I will present how to use Jupyter Notebook through Anaconda, but feel free to use another programming environment. 
- Install Anaconda Python 3.7 version from the [Anaconda](https://www.anaconda.com/download/) website.
- Launch Anaconda Navigator and install Jupyter notebook
- Launch Jupyter notebook

- Follow the instructions:

![](.beginners_guide_to_the_mlhosting_tutorials_images\f3167d77.png)

- Create a new notebook from this page in your browser

![](.beginners_guide_to_the_mlhosting_tutorials_images\cc21bb5a.png)

- Install the Cognite SDK and other packages missing packages (e.g. matplotlib).
 The method below can be executed directly into the Jupyter notebook, and also work if the notebook is in a virtual environment. 
```
# Install a pip package in the current Jupyter kernel
import sys
!{sys.executable} -m pip install cognite
```



## Additions to the tutorials

In general I would recommend adding your name + tutorial in the description of all models/versions etc you make. This
makes it much easier to delete all the tutorial content when you are done. The [Cognite SDK](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#analytics) contains an overview of lines of code which might prove useful. Notice that not all commands
are available for all versions of the SDK. e.g. post_time_series is only available in v05 at
the moment.

To create the source package in the tutorial, be sure that the files are located in your work directory. You can
check your work directory like this:
```
cwd = os.getcwd()
print(cwd)
```


To retrieve api-key as environment variable in the tutorial:
```
API_KEY = os.environ.get("apikeyml")
```
To see the ID of a model/version/source_package, you can use the print function:

![](.beginners_guide_to_the_mlhosting_tutorials_images\a563d2d6.png)

### Time series - Schedule tutorial
In the schedule tutorial you are asked to insert your own time series. Below is the code needed to create
an empty time series for "abc_predicted_production_rate" and post it to CDP.
```
#importing packages
from cognite.v05.timeseries import post_time_series
from cognite.v05.dto import TimeSeries
from cognite.v05.timeseries import get_timeseries
from cognite.v05.timeseries import delete_time_series

#create and post time series
post_time_series([TimeSeries("insert name of time series here")])
```
Check the id of the time series.
```
get_timeseries("insert name of time series here").to_json()
```

Next up you can create time series with data, or you can copy inn the IDs provided below, which are 4 arbitrary time series from CDP. 

```
ts_ids = {
    'abc_temp': 2392036094196256,
    'abc_pressure': 8896263225650691,
    'abc_rpm': 7648214955631357,
    'abc_production_rate': 4269525930539809,
    'abc_predicted_production_rate': "Enter ID of your time series made above"
}
``` 

### Cleaning up
To remove the data you have posted to CDP when you are done with the tutorial you can
log into [this portal](https://modelhosting.cogniteapp.com) to remove source packages and models. Begin by deleting models, and then source packages.
Versions and schedules will be deleted automatically if you follow this process as they are saved as objects under models. 

Finally you can delete your "home made" time series. Note that you must use name and not ID, as name is the unique identifier in v05 (ID is the unique identifier in v06)
```
delete_time_series("insert name of time series here")
```
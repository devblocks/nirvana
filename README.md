# Nirvana

Project Nirvana

## Getting Started

The project currently only runs on Ubuntu. Specifically, Ubuntu 16.04. Due to incompatibilities with Docker and Windows Subsystem Linux (WSL), the package could not be run on Windows.

Run the following instructions on the Linux Ubuntu 16.04 computer.

For the app to work properly, the `important.py` file must be downloaded to this computer. Please do this now.

To install this package, go to the top right of this repository and click "Clone or Download". Click "Download ZIP".

Now open a terminal by pressing the following keys simultaneously:

```
<Ctrl> <Alt> T
```

Copy and paste the following into the terminal:

```
cd Downloads && sudo apt install unzip && unzip nirvana-master.zip && cd nirvana-master && mv ../important.py app/ && ./install.sh && ./run.sh
```

The above commands install docker for Redis and all dependencies. After running the command, the app will be running on `localhost:5000`.

The landing page allows you to specify two dates. The first date is the
start of the range of marketing activities to look at, and the second
date is the end of the range.
The next field is the marketing attribution range, which sets the number
of days after a marketing event that a sale opportunity will be tied to
the marketing activity.

The fourth field sets the same value, but for marketing lists. This is
because marketing list may need a longer range, since marketing material
doesn't go out the first day that the event is created.

The last field allows for a JSON file to be uploaded to the server to sort
the marketing activities into their proper categories.

The "Generate Report" button start the main function, sending API calls to
HubSpot's and Salesforce's servers and cross-references the data for
the marketing attribution. The initial generation of the report will take
the longest. Afterwards, the API reponses will be cached, and the function
will run to completion in under a minute.

Once generated, a table of each sale opportunity will be displayed. This
table can be download in a sorted format with the "Download Report"
button. The config file can also be downloaded. The config file will have
have the original sorting branches inputted and will list all activities
that weren't found in the config file as ['Unlabeled'].

If the redis, celery, and flask servers are left running, the server will
refresh its cached data for the last year of marketing activities.

---

# Issues to be aware of

In `app/Nirvana.py`, there is an upper bound and a lower bound. These are the first and last deal ids to look for when collecting deals from Hubspot. The upper bound need to be updated every month to find new deals, but can't be set higher than the last deal or it will cause an error.

At present, the code looks through 2019 deals, and will need to be changed for 2020. The last deal can be found using the `all_dealGetter()` method in `app/Nirvana.py`. Execute a `GET` request on the hard coded URL, increasing the offset until the last page with a deal is found. Once an approximate number is found, that will return the last page of deals, the upperbound in `app/Nirvana.py` can be changed to that number.

When generated reports, it has also been observed that after changing the settings of the report to generate, a previous report will be generated. The current workaround is to generate a report twice if the results appear strange. The root cause is believed to be some sort of caching.

The "Upload File..." text on the Landing page does not change once the file
is uploaded.

The browser interface currently doesn't have any error handling, and will
not display if an error caused the report to not generate.

If Firefox is your default browser, you may need to disable caching for the
downloaded report to match the report displayed in the browser.

For the date range "12/01/17" to "03/31/18", there are three deals that are
being found twice, though all other duplicates are being handled.

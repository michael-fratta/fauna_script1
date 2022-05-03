A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that, essentially, updates (and adds to) a database (Fauna) from the contents of a CSV file fetched from an SFTP server. The steps it follows are explained - concisely - below (see code for full detail):

• connects to an SFTP server, using the pysftp library, and searches its contents for the most recent CSV file matching the provided search string

• if a file is found, it is sorted and shaped in such a way to match the Fauna objects it will later be compared to, and stored in a list of dictionary objects

• connects to Fauna using the Fauna API, and iterates through a given document collection in Fauna - capturing the unique identifier values needed for later comparison, and appending them to a list

• iterates through this list, using the previous list of objects obtained from the CSV file as a comparator - and separates the (CSV) list of objects into two other lists of objects: one with matching unique identifiers, and one with unmatched (new) unique identifiers

• adds each of the objects contained within the latter, aforementioned, list of new objecs to the document collection in Fauna, via the Fauna API

• compares the subarrays contained within the list of objects with a matching unique identifier - between Fauna and the CSV file - created earlier, with a corresponding subarray within the document collection in Fauna: and updates any mismatched values in Fauna with those from the CSV file

• programmatically refreshes the cache of web apps connected to the relevant Fauna collections - to display the updated data (on front end tools)

• posts relevant updates/actions to a dedicated Slack (messaging service) channel, as a message, via the Slack API.

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.

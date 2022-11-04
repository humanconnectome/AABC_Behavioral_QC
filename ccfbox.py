import os
import io
import sys
from multiprocessing.dummy import Pool
import pandas as pd
import typing
from boxsdk import JWTAuth, OAuth2, Client
from boxsdk.object.file import File
from boxsdk.object.item import Item

from memoizable import Memoizable
from config import LoadSettings

config = LoadSettings()

default_cache = config["root"]["cache"]
default_config = config["config_files"]["box"]


class LifespanBox:
    def __init__(
        self,
        cache=default_cache,
        user="Lifespan Automation",
        config_file=default_config,
    ):
        self.user = user
        self.cache = cache
        self.config_file = config_file
        if not os.path.exists(cache):
            os.makedirs(cache, exist_ok=True)
        self.client = self.get_client()

    def get_client(self):
        auth = JWTAuth.from_settings_file(self.config_file)
        admin_client = Client(auth)

        lifespan_user = None
        # lifespan_user = client.create_user('Lifespan Automation')
        for user in admin_client.users():
            if user.name == self.user:
                lifespan_user = user

        if not lifespan_user:
            print(self.user + " user was not found. Exiting...")
            sys.exit(-1)

        return admin_client.as_user(lifespan_user)

    def get_dev_client(self):
        # Dev access token, active for 1 hour. Get new token here:
        # https://wustl.app.box.com/developers/console
        auth = OAuth2(client_id="", client_secret="", access_token="")
        return Client(auth)

    def list_of_files(
        self,
        folder_ids: typing.List[typing.Union[int, str]],
        includes_file_extension: str = ".csv",
        search_subfolders: bool = True,
    ) -> typing.List[dict]:
        """Recursively list all files in a list of folders.

        Args:
            folder_ids: List of folder ids
            includes_file_extension: File extension to include in the list
            search_subfolders: Recursively search subfolders

        Returns:
            List of files
        """
        result = {}

        for folder_id in folder_ids:

            f = self.client.folder(folder_id)
            # print('Scanning %s' % folder_id)
            print(".", end="")
            items = list(f.get_items())

            folder_ids = []
            files = {}

            for i in items:
                if i.type == "file":
                    if i.name.endswith(includes_file_extension):
                        files[i.id] = {
                            "filename": i.name,
                            "fileid": i.id,
                            "sha1": i.sha1,
                        }
                elif i.type == "folder":
                    folder_ids.append(i.id)

            result.update(files)
            if search_subfolders:
                result.update(self.list_of_files(folder_ids, includes_file_extension, True))

        return result

    def folder_info(self, folder_id):
        f = self.client.folder(folder_id=str(folder_id)).get()
        print("folder owner: " + f.owned_by["login"])
        print("folder name: " + f["name"])

    def get_files(self, folder_id, pattern=None, maxfiles=None):
        """Gets all files in a folder matching pattern up to maxfiles
        :pattern - Can be any string and can contain '*' for wildcards
        :maxfiles - May return slightly more than this due to the offset value
            and pattern matching
        """
        limit = 1000
        if maxfiles and maxfiles < limit:
            limit = maxfiles
        offset = 0
        root_folder = self.client.folder(folder_id=str(folder_id))
        files = []

        while True:
            items = root_folder.get_items(limit=limit, offset=offset)

            for f in items:
                if f.type != "file":
                    continue
                if not pattern:
                    files.append(f)
                elif self._match(f.name, pattern):
                    files.append(f)

            # We either exhausted the listing or have reached maxfiles
            if not items:
                break
            if maxfiles and len(files) >= maxfiles:
                break

            offset += limit

        return files

    def search(
        self,
        pattern,
        limit=100,
        maxresults=1000,
        exclude=None,
        ancestor_folders=None,
        file_extensions=None,
    ):
        """
        Extends box search to narrow down based on glob like pattern
        Exclusions can be specified as comma separated string, like 'Not,This'
        """
        offset = 0
        results = []

        print('looking for "{}" ...'.format(pattern))
        result = self.client.search().query(
            pattern,
            limit=limit,
            offset=offset,
            ancestor_folders=ancestor_folders,
            file_extensions=file_extensions,
        )
        results.extend(result)

        matches = []

        for r in results:
            match = True
            for substr in pattern.split("*"):
                if substr not in r.name:
                    match = False
            if match:  # and exclude and exclude not in r.name:
                if not exclude:
                    matches.append(r)
                else:
                    exclusions = exclude.split(",")
                    included = True
                    for exclusion in exclusions:
                        if exclusion in r.name:
                            included = False
                    if included:
                        matches.append(r)

        return matches

    def get_file_by_id(self, file_id: typing.Union[str, int]) -> File:
        """
        Get box file instance.
        To get content use `.content()`.
        Other metadata is available via `.get()`.

        Args:
            file_id: Box file id

        Returns:
            Box file instance
        """
        return self.client.file(file_id=str(file_id))

    def get_metadata_by_id(self, file_id: typing.Union[str, int]) -> Item:
        """
        Get box file metadata.

        Args:
            file_id: Box file id

        Returns:
            Box file metadata
        """
        return self.get_file_by_id(file_id).get()

    def read_file_in_memory(self, file_id: typing.Union[str, int]) -> File:
        """Bypasses the local filesystem, returns an inmemory buffer"""
        print("Reading file in memory", file_id)
        return self.get_file_by_id(file_id).content()

    def read_io(self, file_id: typing.Union[str, int]) -> io.BytesIO:
        """Bypasses the local filesystem, returns an inmemory file handle"""
        return io.BytesIO(self.read_file_in_memory(file_id))

    def read_csv(self, file_id):
        """Read a csv file into a pandas dataframe, without storing a cached version."""
        return pd.read_csv(self.read_io(file_id))

    def read_excel(self, file_id):
        """Read an excel file into a pandas dataframe, without storing a cached version."""
        return pd.read_excel(self.read_io(file_id))

    def read_text(self, file_id):
        """Read a text file into a string, without storing a cached version."""
        f = self.read_file_in_memory(file_id)

        try:
            return f.decode("UTF-16")
        except UnicodeDecodeError:
            return f.decode("UTF-8")

    def download_file(self, file_id, download_dir=None, override_if_exists=False):
        """
        Downloads a single file to cache space or provided directory
        """
        download_dir = download_dir or self.cache

        file = self.get_file_by_id(file_id)
        path = os.path.join(download_dir, file.get().name)

        if os.path.exists(path) and not override_if_exists:
            return path

        with open(path, "wb+") as fd:
            fd.write(file.content())

        return path

    def download_files(self, file_ids, directory=None, workers=20):
        """
        Takes a list of file ids and downloads them all to cache space or user
        specified directory
        """
        if directory:
            self.cache = directory
        pool = Pool(workers)
        filepaths = pool.map(self.download_file, file_ids)
        pool.close()
        pool.join()
        return filepaths
        # Euivalent to this for loop
        # for f in file_ids:
        #     self.download_file(f)

    def upload_file(self, source_path, folder_id):
        """
        Upload a new file into an existing folder by folder_id.
        """
        file = self.client.folder(str(folder_id)).upload(source_path)
        print(file)
        return file

    def update_file(self, file_id, file_path, rename=True):
        """
        Alias of `update_version`.
        """
        return self.update_version(file_id, file_path, rename)

    def update_version(self, file_id, file_path, rename=True):
        """
        Upload a new version of an existing file by file_id
        """
        base = os.path.basename(file_path)
        file = self.client.file(str(file_id))
        f = file.update_contents(file_path)

        if rename and file.get().name != base:
            file.rename(base)

        return f

    @staticmethod
    def _match(string, pattern):
        match = True
        for substr in pattern.split("*"):
            # Skip "empty" matches
            if not substr:
                continue

            if substr not in string:
                # print(substr)
                match = False
        return match

    def Box2dataframe(self, curated_fileid_start):
        """
        A Legacy function.
        """

        # get current best curated data from BOX (a csv with one header row)
        # and read into pandas dataframe for QC
        raw_fileid = curated_fileid_start
        data_path = box.download_file(raw_fileid)
        raw = pd.read_csv(data_path, header=0, low_memory=False, encoding="ISO-8859-1")
        # raw['DateCreatedDatetime']=pd.to_datetime(raw.DateCreated).dt.round('min')
        # raw['InstStartedDatetime']=pd.to_datetime(raw.InstStarted).dt.round('min')
        # raw['InstEndedDatetime']=pd.to_datetime(raw.InstEnded).dt.round('min')
        return raw


class CachedBoxFileReader(Memoizable):
    def __init__(self, cache_file=".box_cache", expire_in_days=1, box: LifespanBox = None, **kwargs):
        self.box = box
        self.kwargs = kwargs
        super().__init__(cache_file=cache_file, expire_in_days=expire_in_days)

    def run(self, file_id: typing.Union[int, str]) -> io.BytesIO:
        if self.box is None:
            self.box = LifespanBox(**self.kwargs)
        return self.box.read_file_in_memory(file_id)

    def read_csv(self, csv_file_id):
        """Read a csv file into a pandas dataframe."""
        return pd.read_csv(self.__call__(csv_file_id))

    def read_excel(self, csv_file_id):
        """Read an excel file into a pandas dataframe."""
        return pd.read_excel(self.__call__(csv_file_id))

    def read_text(self, text_file_id):
        """Read a text file into a string."""
        return self.__call__(text_file_id).getvalue().decode("utf-8")

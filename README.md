# Discord image extractor
a tool for extracting the memes (images and videos) you shared on discord

## how to use
edit the config.ini file to point to where you want the files to end up in `OutputLocation`
and where is the export zip `SourceZip`, 
or if you extracted it you can also comment out the zip option with a `;` and set the `SourceFolder` calue

### TODO
- [x] ~~get data from zip~~
- [ ] save data to zip or tarball
- [x] ~~make loading part async~~
- [ ] put data on guilds in db
- [ ] make gui to browse data
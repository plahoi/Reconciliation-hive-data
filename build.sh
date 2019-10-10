# Builds all dependencies into one .zip file

cd recon

rm package.zip

zip -9rv package.zip policies src

exit 0
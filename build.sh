# Builds all dependencies into one .zip file

cd recon

rm package.zip

zip -9mv package.zip policies src

exit 0
#!/bin/bash
set -e
set -o pipefail


TARGET_VER="$1"
TARGET_PATH="$2"
END_PATH="$3"

set -u 
if [[ $TARGET_PATH == "" ]]; then
	echo "undefined target path expect it as <script> version path"
	exit 1
fi

if [[ $TARGET_VER == "" ]]; then
	echo "undefined target ver expect it as <script> version path"
	exit 1
fi



build_kubernetes(){
	cd "$TARGET_PATH/kubernetes"
	if [[ "$TARGET_VER" != "$(git branch | grep -F '*' | awk '{print $2}')" ]]; then
		if [[ "" == "$(git branch | grep ${TARGET_VER})" ]]; then
			echo "switching to $TARGET_VER (creating branch)"
			git checkout tags/${TARGET_VER} -b ${TARGET_VER}
		else
			echo "switching to $TARGET_VER"
			git checkout ${TARGET_VER}
		fi
	fi
	if [[ ! -d "$TARGET_PATH/kubernetes/_output" ]]; then
		echo "* _output dir does not exist. building kubernetes (generated items only)"
		make generated_files gen_openapi
		return_to_path
	else
		 echo "* _output dir exists. not building"
	fi
}

return_to_path(){
	if [[ $END_PATH != "" ]]; then
		echo "* end process at $END_PATH"
		cd $END_PATH
	fi
}

echo "*************************************************************************"
echo "* ensure kubernetes exists for e2e tests" 
echo "* Path: ${TARGET_PATH}"
echo "* Version: ${TARGET_VER}"
echo "*************************************************************************"
if [ -d ${TARGET_PATH} ]; then 
	echo "* dirctory ${TARGET_PATH}"
	echo "* found and is NOT EMPTY .. NOT CLONING"
	build_kubernetes
	exit 0
fi

echo "* ensure directory: ${TARGET_PATH}"
mkdir -p ${TARGET_PATH}


echo "* change dir to:${TARGET_PATH}"
cd ${TARGET_PATH}
echo "* cloning ${TARGET_VER} with depth=1"
git clone  https://github.com/kubernetes/kubernetes.git

build_kubernetes
return_to_path

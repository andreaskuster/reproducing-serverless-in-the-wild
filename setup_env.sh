#!/usr/bin/env bash

#
# Copyright 2021-22 Andreas Kuster, Wireless And Networked Distributed Sensing Group, Nanyang Technological University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# install required packages
xargs sudo apt-get -y install < ubuntu_requirements.txt

# install python virtual environment
git submodule update --init --recursive
python3 -m venv venv/
source venv/bin/activate
python3 -m pip install -r requirements.txt
# add project to python path
export PYTHONPATH=$PYTHONPATH:`pwd`

## Copyright (C) 2015 surbhi
## 
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with Octave; see the file COPYING.  If not, see
## <http://www.gnu.org/licenses/>.

## first

## Author: surbhi <surbhi@surbhi-Compaq-610>
## Created: 2015-01-25

function [] = first ()

clc;
clear;
data = dlmread("/home/surbhi/Desktop/dataset/Compound.txt", "\t");
value=unique(data(:,4));
data2=sortrows(data,4);
size1=length(value);

firstindex=find(data2(:,4)==1,1);
for i =2:size1
	firstindex=[firstindex find(data2(:,4)==i,1)];
	
end
#data2
##cmap=hsv(size1);
firstindex
##for j = 1:size1
	##if j==size1
		

		
		
	##else
		plot(data2(1:50,2),data2(1:50,3),'r*');
		hold on;
		plot(data2(51:142,2),data2(51:142,3),'g*');
		hold on;
		plot(data2(143:180,2),data2(143:180,3),'c*');
		hold on;
		plot(data2(181:225,2),data2(181:225,3),'m*');
		hold on;
		plot(data2(226:383,2),data2(226:383,3),'k*');
		hold on;
		#plot(data2(223:225,1),data2(223:225,2),'y*');
		#hold on;
		#plot(data2(226:383,1),data2(226:383,2),'b*');
		#hold on;
		plot(data2(384:399,2),data2(384:399,3),'y*');
		hold on;

		##plot(data2(350:399,1),data2(350:399,2),'y*');
		
	##SSend
		#hold on;


end






endfunction

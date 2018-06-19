
function Main()
clc;
clear all;
persistent h;
% This function will read the initial partition and will calculate the
% sorted matrix i.e matrix a and matrix b of slices division for each data
% point.
u= csvread('/home/surbhi/Desktop/dataset/zahn_compound.csv');
m=u';



Calculate(m);
end

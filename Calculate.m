function []= Calculate(m)
%clc;


% This function will calculate the matrix for total no. of points in each
% slice and also accumulative points matrix.
% matrix b defines the slice for each data point in each dimension
% matrix c defines the number of points in each slice
% matrix d calculates the accumulative points

%h=h+1;
n = size(m,2);          % No. of columns in the matrix
l = size(m,1);          % No. of rows in the matrix
eps = 7;             	 % value of eps can be varied and it changes the result.width od the slice

%for i = 1 : n                       
    %a(i,1:l)= sort( m(:,i));
%end

part=0;
p=sort(m');             % a is the matrix of data with sorted values of each dimension
a=p';
% matrices for slice is created for the initial partition and output is the
% matrix b

s=size(a,1);
t=size(a,2);


for i = 1 : s
    start = a(i,1);
    index=1;                                        % index stores the value of slice number                                     
    
    for j = 1 : t
        if a(i,j)<(start+(2*eps))
            b(i,j) = index;
        
        elseif a(i,j)>=(start+(2*eps))
            
            while a(i,j)>=(start+(2*eps))
                start = start+(2*eps);
                index=index+1;
            end
            if a(i,j)<(start+(2*eps))
                b(i,j)=index;
            end
        end
    end
end

s=size(a,1);
t=size(a,2);



count=0;                                        % count stores the value of number of points in each slice
ind=0;                                           % counting the number of points in each slice.
for i= 1: s
    ind=b(i,1);
    count=0;
    for j=1:t
        
        if b(i,j) ~= ind
            c(i,ind)=count;
            count=0;
            ind=b(i,j);
        end
        if b(i,j)==ind
            count=count+1;
            ind=b(i,j);
        end
    end
    c(i,ind)=count;
end
x=size(c,1);
y=size(c,2);

total=0;
for i=1:x                                   % d matrix contains the accumulative points in each slice
    for j=1:y
        total=total+c(i,j);
        d(i,j)=total;
    end
    total=0;
end
a
b
c
d

Best_slice(a,b,c,d,m);

end

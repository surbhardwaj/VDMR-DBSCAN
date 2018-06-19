function []= Best_slice(a,b,c,d,m)
%clc;

% This function will find the best slice with minimum number of points and
% will divide the data into two partitions
persistent h;


x=size(c,1);
y=size(c,2);
s=size(a,1);
t=size(a,2);
state=0;

dim_index=0;                                % finds the dimension with min no. of points
slice_index=0;                              % slice with min no of points
min_pts=0;                                  % min. no of points in the slice

flagcheck=0;                                         % flag=1 means atleast once dimension has slicecount more than 30.
se=sum(c(1,:));                             % se is the total no. of points in the partition
beta = 100;
theta=0.3;                                  % theta should be between 0 and 0.5, to achieve load balance
slicecount=20;
min=max(d(:));                               % finds the maximum value in the partition matrix
if se >= beta                               % number of points in each partition should be greater than beta
   
    for i= 1:x                              % Checking for each dimension in the partition
        if b(i,t)>=slicecount                % number of slice count in each dimension should be greater than a threshold
            
            
            n=b(i,t);
            for k=1:n-2                     % to avoid selecting the boundary slice
                if (se*(1-theta))>d(i,k) && d(i,k)>(se*theta)
                        %i
                        %k
                        if c(i,k)~=0
                            flagcheck=1;
                            if c(i,k)<min
                                min=c(i,k);
                                min_pts=c(i,k);
                                slice_index=k;
                                dim_index=i;
                             end
                        end
                end
               
            end
        end
            
    end

end

if flagcheck==0
    if isempty(h)==1
        h=0;
    end
    h=h+1;
    %flagcheck
    e=(zeros(t,1)+h);
    f=[m',e];
    
    dlmwrite('/home/surbhi/Desktop/Final_Partition.csv',f,'-append');            % matrix 'm' is written to the file because each dimension has no. of slices less than slice count
   
end
        
flagcheck
min_pts      % minimum points in the slice with dimension and slice index.
slice_index
dim_index

% finding the points which are lying in (slice_index) slice of (dim_index)
% dimension

if flagcheck==1 
   val=find(b(dim_index,:)==slice_index);
   start=val(1);                       % start gives the starting index of the 
   last=val(end);    
   rec1=a(dim_index,start);            % rec1 stores the value where the slice starts for the dimension
   rec2=a(dim_index,last);             % rec2 stores the value where the slice ends for the dimension
   o=(sortrows(m',dim_index))'         % this will sort the matrix according to the dim_index dimension
   y1=find(o(dim_index,:)==rec1);       % y1 stores the value where the slice starts and rec2 stores the value where slice ends.
  
   y2=find(o(dim_index,:)==rec2);
   m1=o(:,1:1:last);
   m2=o(:,start:1:end);
   
   %m1=a(:,1:1:last);
   %b1=b(:,1:1:last);                    % b1 and b2 are the aprtitioned slice matrices.
   %m2=a(:,start:1:end);
   %b2=b(:,start:1:end);
   %num=b2(dim_index,1);
   %p=num-1;                              % difference to be subtracted from each value of the matrix
    
   %q=size(b2,2);                          % number of columns in b2
    
    
  %  updating the slice matrix, the row on the basis of which partition is
    % done
    
    %for i=1:q                             
     %   b2(dim_index,i)=b2(dim_index,i)-p;
    %end
    %m1
    %m2
    %b1 
    %b2
    
    Calculate(m1);
    Calculate(m2);
    
    
end


end

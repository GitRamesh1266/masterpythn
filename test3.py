
inp_str = "GeeksforGeeks"

freq = {} 
  
for ltters in inp_str:
    
    if ltters in freq: 
        freq[ltters] += 1
    else: 
        freq[ltters] = 1

print(freq) 
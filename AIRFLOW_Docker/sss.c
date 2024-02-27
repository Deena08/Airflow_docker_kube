#include <stdio.h>
#include <string.h>
#include <ctype.h>


void reverseAndCapitalize(char *word) {
    int length = strlen(word);
   
    for (int i = 0; i < length / 2; i++) {
        char temp = word[i];
        word[i] = word[length - i - 1];
        word[length - i - 1] = temp;
    }
    word[0] = toupper(word[0]);
}


char* reverseString(char *input) {
    int length = strlen(input);
    int start = 0;

  
    for (int i = 0; i <= length; i++) {
     
        if (input[i] == ' ' || input[i] == '\0') {
         
            reverseAndCapitalize(input + start);
  
            start = i + 1;
        }
    }
    return input;
}

int main() {
    char input[100];
    
    fgets(input, sizeof(input), stdin);

   
    if (input[strlen(input) - 1] == '\n') {
        input[strlen(input) - 1] = '\0';
    }

   
    char *output = reverseString(input);

   
    printf("Output: %s\n", output);

    return 0;
}
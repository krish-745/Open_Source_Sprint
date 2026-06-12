const fs = require('fs');

function keepMain(filePath) {
  let tsCode = fs.readFileSync(filePath, 'utf8');
  tsCode = tsCode.replace(/<<<<<<< HEAD[\s\S]*?=======\n/g, '');
  tsCode = tsCode.replace(/>>>>>>> main\n/g, '');
  fs.writeFileSync(filePath, tsCode);
}

keepMain('src/services/task-scheduler.ts');
keepMain('src/services/__tests__/task-scheduler.test.ts');
console.log('Fixed conflicts');

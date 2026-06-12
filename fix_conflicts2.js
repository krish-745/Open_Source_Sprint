const fs = require('fs');

function keepMain(filePath) {
  if (!fs.existsSync(filePath)) return;
  let tsCode = fs.readFileSync(filePath, 'utf8');
  tsCode = tsCode.replace(/<<<<<<< HEAD[\s\S]*?=======\r?\n/g, '');
  tsCode = tsCode.replace(/>>>>>>> main\r?\n?/g, '');
  fs.writeFileSync(filePath, tsCode);
}

keepMain('src/services/task-scheduler.ts');
keepMain('src/services/__tests__/task-scheduler.test.ts');
console.log('Fixed conflicts again');

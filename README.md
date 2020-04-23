## Usage

```javascript
const nr = require('neverred-function');

nr.invoke((event, context, logger) => {
  const input = event.number;
  const result = input ** 2;
  logger.info({ input, result });
  return JSON.stringify({result});
});
```

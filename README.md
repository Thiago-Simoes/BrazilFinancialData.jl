# BrazilFinancialData.jl
### A Julia Package to get data from BrazilianFinancial APIs.

This project is under development and totally free-to-use, feel free to contribute. I'm a student so forgive any possible problem and report it opening a issue.
Ideas can also, and should, be sent.

> # Installing
> 1. In Julia terminal, activate your enviroment.
> 2. Paste `using Pkg`.
> 3. Paste `Pkg.add("BrazilFinancialData")`
> 4. That's all! 🎉🎉 


>## Release notes:
> ---
> ### **v0.1.4**:
> Based on Python lib brFinance(@eudesrodrigo) now implemented the functionality of download company financial statements directly from Julia using HTTP.jl and WebScrapping packages.   
> Now, the package uses brFinance only for one specific functionality that I couldn't get to work. ASAP I will release a full Julia version.
> 
>**Financial statements**:
>    - Balanço Patrimonial Ativo (Balance sheet - Assets)
>    - Balanço Patrimonial Passivo (Balance sheet - Liabilities)
>    - Demonstração do Resultado (Income statement)
>    - Demonstração do Resultado Abrangente
>    - Demonstração do Fluxo de Caixa (Cash flow statement)
>    - <s>Demonstração das Mutações do Patrimônio Líquido</s> (pending implementation)
>    - Demonstração de Valor Adicionado
  
  
## Actual features
> ### CVM:
> - Get daily information of investment funds (by month or period)
> - Get statement of investment funds (by year or period)
> *Both data can be for a specific fund.

> ### Bacen:
> Get data for the following series:
> - IPCA  
> - SELIC  
> - USD_BRL  
> - CAGED  
> - SINAPI  
> - PIB  
> - IC_BR  
> - Utilizacao Capacidade Industrial  
> - Desemprego  
> - Divida Liquida  
> - Divida Liquida PIB  
> - Cesta Basica Aracaju  
> - Cesta Basica Belem  
> - Cesta Basica Belo Horizonte  
> - Cesta Basica Brasilia  
> - Cesta Basica Curitiba  
> - Cesta Basica Florianopolis  
> - Cesta Basica Fortaleza  
> - Cesta Basica Goiania  
> - Cesta Basica Joao Pessoa  
> - Cesta Basica Natal  
> - Cesta Basica PortoAlegre  
> - Cesta Basica Recife  
> - Cesta Basica RJ  
> - Cesta Basica Salvador  
> - Cesta Basica SaoPaulo  
> - Cesta Basica Vitoria  


*This package is a work in progress.*


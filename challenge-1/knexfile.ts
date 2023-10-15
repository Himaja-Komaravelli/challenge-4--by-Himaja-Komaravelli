const knexfile=()=>{
    return{
        development: {
            client: 'sqlite3',
            connection: {
              filename: './out/database.sqlite'
            },
            migrations:{
              directory:'./migrations'
            }
          },
    }
}
export default knexfile;


